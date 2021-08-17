import json
from json import JSONDecodeError

from firebase_admin import (delete_app as delete_firebase_instance, get_app as get_firebase_app,
    initialize_app as initialize_firebase_app)
from firebase_admin.credentials import Certificate as FirebaseCertificate

from config.constants import (ANDROID_FIREBASE_CREDENTIALS, BACKEND_FIREBASE_CREDENTIALS,
    FIREBASE_APP_TEST_NAME, IOS_FIREBASE_CREDENTIALS)
from database.system_models import FileAsText


class FirebaseMisconfigured(Exception): pass

#
# Firebase app object instantiation and credential tests
# (This code can probably be simplified with a threading.Lock object.)
#

def safely_get_db_credential(credential_type: str) -> str or None:
    """ just a wrapper to handle ugly code """
    credentials = FileAsText.objects.filter(tag=credential_type).first()
    if credentials:
        return credentials.text
    else:
        return None


def get_firebase_credential_errors(credentials: str):
    """ Wrapper to get error strings for test_firebase_credential_errors because otherwise the
        code is gross.  Returns None if no errors occurred. """
    try:
        test_firebase_credential_errors(credentials)
        return None
    except Exception as e:
        return str(e)


def test_firebase_credential_errors(credentials: str) -> None:
    """ Tests credentials by creating a temporary otherwise unused credential. """
    try:
        encoded_credentials = json.loads(credentials)
    except JSONDecodeError:
        # need clean error message
        raise Exception("The credentials provided are not valid JSON.")

    # both of these raise ValueErrors, delete only fails if cert and app objects pass.
    cert = FirebaseCertificate(encoded_credentials)
    app = initialize_firebase_app(cert, name=FIREBASE_APP_TEST_NAME)
    delete_firebase_instance(app)


def check_firebase_instance(require_android=False, require_ios=False) -> bool:
    """ Test the database state for the various creds. If creds are present determine whether
    the firebase app is already instantiated, if not call update_firebase_instance. """

    active_creds = list(FileAsText.objects.filter(
        tag__in=[BACKEND_FIREBASE_CREDENTIALS, ANDROID_FIREBASE_CREDENTIALS, IOS_FIREBASE_CREDENTIALS]
    ).values_list("tag", flat=True))

    if (    # keep those parens.
            BACKEND_FIREBASE_CREDENTIALS not in active_creds
            or (require_android and ANDROID_FIREBASE_CREDENTIALS not in active_creds)
            or (require_ios and IOS_FIREBASE_CREDENTIALS not in active_creds)
    ):
        return False

    if get_firebase_credential_errors(safely_get_db_credential(BACKEND_FIREBASE_CREDENTIALS)):
        return False

    # avoid calling update so we never delete and then recreate the app (we get thrashed
    # during push notification send from calling this, its not thread-safe), overhead is low.
    try:
        get_firebase_app()
    except ValueError:
        # we don't care about extra work inside calling update_firebase_instance, it shouldn't be
        # hit too heavily.
        update_firebase_instance()

    return True


def update_firebase_instance(rucur_depth=3) -> None:
    """ Creates or destroys the firebase app, handling basic credential errors. """
    junk_creds = False
    encoded_credentials = None  # IDE complains

    try:
        encoded_credentials = json.loads(safely_get_db_credential(BACKEND_FIREBASE_CREDENTIALS))
    except (JSONDecodeError, TypeError):
        junk_creds = True

    try:
        delete_firebase_instance(get_firebase_app())
    except ValueError:
        # occurs when get_firebase_app() fails, delete_firebase_instance is only called if it succeeds.
        pass

    if junk_creds:
        return

    # can now ~safely initialize the firebase app, re-casting any errors for runime scenarios
    # errors at this point should only occur if the app has somehow gotten broken credentials.
    try:
        cert = FirebaseCertificate(encoded_credentials)
    except ValueError as e:
        raise FirebaseMisconfigured(str(e))

    try:
        initialize_firebase_app(cert)
    except ValueError as e:
        # occasionally we do hit a race condition, handle that with 3 tries, comment in error message.
        if rucur_depth >= 0:
            return update_firebase_instance(rucur_depth - 1)
        raise FirebaseMisconfigured(
            "This error is usually caused by a race condition, please report it if this happens frequently: "
            + str(e)
        )
