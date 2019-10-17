import functools

from flask import abort, request

from database.common_models import is_object_id
from database.study_models import Study
from database.user_models import Researcher, StudyRelation

####################################################################################################
################################# Primary Access Validation ########################################
####################################################################################################


def data_access_determine_study_access(some_function):
    @functools.wraps(some_function)
    def wrapper(*args, **kwargs):
        data_access_api_check_researcher_study_access(allow_test_studies=True)
        return some_function(*args, **kwargs)
    return wrapper


def data_access_determine_chunked_data_study_access(some_function):
    @functools.wraps(some_function)
    def wrapper(*args, **kwargs):
        data_access_api_check_researcher_study_access(allow_test_studies=False)
        return some_function(*args, **kwargs)
    return wrapper


def data_access_get_and_validate_researcher() -> Researcher:
    """ returns """
    access_key, secret_key = data_access_get_and_validate_credentials()
    try:
        researcher = Researcher.objects.get(access_key_id=access_key)
    except Researcher.DoesNotExist:
        return abort(403)  # access key DNE

    if not researcher.validate_access_credentials(secret_key):
        return abort(403)  # incorrect secret key

    return researcher


####################################################################################################
################################# Interact with the request ########################################
####################################################################################################


def data_access_get_and_validate_study():
    study_object_id = request.values.get('study_id', None)
    study_pk = request.values.get('study_pk', None)
    return get_and_confirm_study_exists(study_object_id=study_object_id, study_pk=study_pk)


def data_access_get_and_validate_credentials():
    access_key = request.values.get("access_key", None)
    secret_key = request.values.get("secret_key", None)
    if access_key is None or secret_key is None:
        abort(400)
    return access_key, secret_key


def data_access_api_check_researcher_study_access(allow_test_studies=True):
    """ Checks whether the researcher is able to make api request for this study.
    Parameter allows control of whether to allow the api call to hit a test study. """
    study = data_access_get_and_validate_study()
    researcher = get_and_validate_researcher_on_study(study)

    user_exceptions = researcher.site_admin or researcher.is_batch_user
    test_studies_allowed = allow_test_studies or study.is_test

    if not user_exceptions or not test_studies_allowed:
        # You're only allowed to download chunked data from test studies
        return abort(404)
    return study


####################################################################################################
# helpers with parameters
####################################################################################################


def get_and_validate_researcher_on_study(study):
    """
    Finds researcher based on the secret key provided.
    Returns 403 if researcher doesn't exist, is not credentialed on the study, or if
    the secret key does not match.
    """
    researcher = data_access_get_and_validate_researcher()

    # case: batch users and site admins have access to everything.
    # case: researcher is not credentialed for this study
    if (
            not StudyRelation.objects.filter(study_id=study.pk, researcher=researcher).exists()
            and not researcher.site_admin
            and not researcher.is_batch_user
    ):
        return abort(403)

    return researcher


def get_and_confirm_study_exists(study_object_id=None, study_pk=None):
    """
    Checks for a valid study object id or primary key.
    Study object id malformed (not 24 characters) causes 400 error.
    Study does not exist in our database causes 404 error.
    """

    if study_object_id is not None:

        # If the ID is incorrectly sized, we return a 400
        if not is_object_id(study_object_id):
            # print("Received invalid length objectid as study_id in the data access API.")
            return abort(400)

        # If no Study with the given ID exists, we return a 404
        try:
            study = Study.objects.get(object_id=study_object_id)
        except Study.DoesNotExist:
            # print("study '%s' does not exist." % study_object_id)
            return abort(404)
        else:
            return study

    elif study_pk is not None:
        # If no Study with the given ID exists, we return a 404
        try:
            study = Study.objects.get(pk=study_pk)
        except Study.DoesNotExist:
            # print("study '%s' does not exist." % study_pk)
            return abort(404)
        else:
            return study

    else:
        return abort(400)
