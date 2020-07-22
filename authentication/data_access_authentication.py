import functools

from flask import abort, request

from config.study_constants import BASE64_GENERIC_ALLOWED_CHARACTERS, OBJECT_ID_ALLOWED_CHARS
from database.study_models import Study
from database.user_models import Researcher, StudyRelation

class BadObjectIdType(Exception): pass
class IncorrectAPIAuthUsage(Exception): pass


DEBUG = False

def log(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)


def is_object_id(object_id: str) -> bool:
    """ Object IDs, we have random strings in newer objects, so we only care about length. """
    # due to change in django we have to check database queries for byte strings as they get coerced
    # to strings prepended with b'
    if not isinstance(object_id, str) or object_id.startswith("b'"):
        raise BadObjectIdType(str(object_id))

    # need to be composed of alphanumerics
    for c in object_id:
        if c not in OBJECT_ID_ALLOWED_CHARS:
            return False

    return len(object_id) == 24


################################# Primary Access Validation ########################################

def get_api_researcher_and_study() -> (Researcher, Study):
    return get_api_researcher(), get_api_study()


def get_api_study() -> Study:
    try:
        return request.api_study
    except AttributeError:
        raise IncorrectAPIAuthUsage("request.api_study used before/without credential checks.")


def get_api_researcher() -> Researcher:
    try:
        return request.api_researcher
    except AttributeError:
        raise IncorrectAPIAuthUsage("request.api_researcher used before/without credential checks.")


def api_credential_check(some_function: callable):
    """ Checks API credentials and attaches the researcher to the request object. """
    @functools.wraps(some_function)
    def wrapper(*args, **kwargs):
        request.api_researcher = api_get_and_validate_researcher()  # validate and cache
        return some_function(*args, **kwargs)
    return wrapper


def api_study_credential_check(conditionally_block_test_studies:bool=False) -> callable:
    """ Decorate api-credentialed functions to test whether user exists, has provided correct
     credentials, and then attach the study and researcher to the request. """
    def the_decorator(some_function: callable):
        @functools.wraps(some_function)
        def the_inner_wrapper(*args, **kwargs):
            study, researcher = api_test_researcher_study_access(conditionally_block_test_studies)
            # cache researcher and study on the request for easy database-less access
            request.api_study = study
            request.api_researcher = researcher
            return some_function(*args, **kwargs)
        return the_inner_wrapper
    return the_decorator


def api_get_and_validate_researcher() -> Researcher:
    """ returns """
    access_key, secret_key = api_get_and_validate_credentials()
    try:
        researcher = Researcher.objects.get(access_key_id=access_key)
    except Researcher.DoesNotExist:
        log("no such researcher")
        return abort(403)  # access key DNE

    if not researcher.validate_access_credentials(secret_key):
        log("key did not match researcher")
        return abort(403)  # incorrect secret key

    return researcher


################################# Interact with the request ########################################
"""
In general use the decorators. These functions are the underlying components of those decorators,
they are complex and easy to misuse.
"""


def api_test_researcher_study_access(block_test_studies: bool) -> (Study, Researcher):
    """ Checks whether the researcher is allowed to do api access on this study.
    Parameter allows control of whether to allow the api call to hit a test study. """
    study = api_get_study_confirm_exists()
    researcher = api_get_validate_researcher_on_study(study)

    user_exceptions = researcher.site_admin or researcher.is_batch_user
    do_block = block_test_studies and not study.is_test

    if not user_exceptions and do_block:
        # You're only allowed to download chunked data from test studies, otherwise doesn't exist.
        log("study not accessible to researcher")
        return abort(404)

    return study, researcher


def api_get_and_validate_credentials() -> (str, str):
    """ Sanitize access and secret keys from request """
    access_key = request.values.get("access_key", None)
    secret_key = request.values.get("secret_key", None)

    # reject empty strings and value-not-present cases
    if not access_key or not secret_key:
        log("missing cred")
        return abort(400)

    # access keys use generic base64
    for c in access_key:
        if c not in BASE64_GENERIC_ALLOWED_CHARACTERS:
            log("bad cred access key")
            return abort(400)
    for c in secret_key:
        if c not in BASE64_GENERIC_ALLOWED_CHARACTERS:
            log("bad cred secret key")
            return abort(400)

    return access_key, secret_key


def api_get_validate_researcher_on_study(study: Study) -> Researcher:
    """
    Finds researcher based on the secret key provided.
    Returns 403 if researcher doesn't exist, is not credentialed on the study, or if
    the secret key does not match.
    """
    researcher = api_get_and_validate_researcher()

    # case: batch users and site admins have access to everything.
    # case: researcher is not credentialed for this study
    if (
            not StudyRelation.objects.filter(study_id=study.pk, researcher=researcher).exists()
            and not researcher.site_admin
            and not researcher.is_batch_user
    ):
        log(f"study found: {StudyRelation.objects.filter(study_id=study.pk, researcher=researcher).exists()}")
        log(f"researcher.site_admin: {researcher.site_admin}")
        log(f"researcher.is_batch_user: {researcher.is_batch_user}")
        log("no study access")
        return abort(403)

    return researcher


def api_get_study_confirm_exists() -> Study:
    """
    Checks for a valid study object id or primary key.
    Study object id malformed (not 24 characters) causes 400 error.
    Study does not exist in our database causes 404 error.
    """
    study_object_id = request.values.get('study_id', None)
    study_pk = request.values.get('study_pk', None)

    if study_object_id is not None:

        # If the ID is incorrectly sized, we return a 400
        if not is_object_id(study_object_id):
            log("bad study obj id: ", study_object_id)
            return abort(400)

        # If no Study with the given ID exists, we return a 404
        try:
            study = Study.objects.get(object_id=study_object_id)
        except Study.DoesNotExist:
            log("study '%s' does not exist (obj id)" % study_object_id)
            return abort(404)
        else:
            return study

    elif study_pk is not None:
        # study pk must coerce to an int
        try:
            int(study_pk)
        except ValueError:
            log("bad study pk")
            return abort(400)

        # If no Study with the given ID exists, we return a 404
        try:
            study = Study.objects.get(pk=study_pk)
        except Study.DoesNotExist:
            log("study '%s' does not exist (study pk)" % study_object_id)
            return abort(404)
        else:
            return study

    else:
        log("no study provided at all")
        return abort(400)
