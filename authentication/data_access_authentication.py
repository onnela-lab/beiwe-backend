import functools
from typing import Tuple

from django.http import UnreadablePostError
from django.http.request import HttpRequest
from django.utils import timezone

from constants.security_constants import BASE64_GENERIC_ALLOWED_CHARACTERS, OBJECT_ID_ALLOWED_CHARS
from database.security_models import ApiKey
from database.study_models import Study
from database.user_models_researcher import Researcher, StudyRelation
from libs.internal_types import ApiResearcherRequest, ApiStudyResearcherRequest, ResearcherRequest
from libs.sentry import make_error_sentry, SentryTypes
from middleware.abort_middleware import abort


class BadObjectIdType(Exception): pass
class IncorrectAPIAuthUsage(Exception): pass


DEBUG_API_AUTHENTICATION = False


def log(*args, **kwargs):
    if DEBUG_API_AUTHENTICATION:
        print("api authentication:", *args, **kwargs)


def is_object_id(object_id: str) -> bool:
    """ Object IDs, we have random strings in newer objects, so we only care about length. """
    # due to change in django we have to check database queries for byte strings as they get coerced
    # to strings prepended with b'
    if not isinstance(object_id, str) or object_id.startswith("b'"):
        log("bad objectid type")
        raise BadObjectIdType(str(object_id))
    
    # need to be composed of alphanumerics
    for c in object_id:
        if c not in OBJECT_ID_ALLOWED_CHARS:
            log("object id disallowed characters")
            return False
    
    return len(object_id) == 24


################################# Primary Access Validation ########################################


def api_credential_check(some_function: callable):
    """ Checks API credentials and attaches the researcher to the request object. """
    @functools.wraps(some_function)
    def wrapper(*args, **kwargs):
        request: ApiResearcherRequest = args[0]
        assert isinstance(request, HttpRequest), \
            f"first parameter of {some_function.__name__} must be an HttpRequest, was {type(request)}."
        # populate the ApiResearcherRequest
        request.api_researcher = api_get_and_validate_researcher(request)  # validate and cache
        return some_function(*args, **kwargs)
    return wrapper


def api_study_credential_check() -> callable:
    """ Decorate api-credentialed functions to test whether user exists, has provided correct
     credentials, and then attach the study and researcher to the request. """
    def the_decorator(some_function: callable):
        @functools.wraps(some_function)
        def the_inner_wrapper(*args, **kwargs):
            request: ApiStudyResearcherRequest = args[0]
            
            # save us from ourselves
            assert isinstance(request, HttpRequest), \
                f"first parameter of {some_function.__name__} must be an HttpRequest, was {type(request)}."
            
            # populate the ApiStudyResearcherRequest
            request.api_study, request.api_researcher = api_check_researcher_study_access(request)
            
            return some_function(*args, **kwargs)
        return the_inner_wrapper
    return the_decorator


def api_get_and_validate_researcher(request: HttpRequest) -> Researcher:
    """ Get keys from the request, get Researcher from keys, get ApiKey from keys, validate keys,
    erroring appropriately along the way. Update the last_successful_use or last_unsuccessful_use
    fields of the ApiKey. """
    
    access_key, secret_key = api_get_and_sanitize_credentials(request)
    try:
        researcher: Researcher = Researcher.objects.get(
            api_keys__access_key_id=access_key, api_keys__is_active=True
        )
    except Researcher.DoesNotExist:
        log("no such researcher/key/inactive key")
        return abort(403)  # access key DNE
    
    try:
        api_key = ApiKey.objects.get(access_key_id=access_key, researcher=researcher, is_active=True)
    except ApiKey.DoesNotExist:
        # this case should be unreachable, report it if it happens I guess?
        log("researcher has no such key - unreachable??")
        with make_error_sentry(SentryTypes.elastic_beanstalk):
            raise IncorrectAPIAuthUsage("Researcher has no such key")
        return abort(500)  # error sentry swallows errors, this code runs when the error is raised.
    
    if not api_key.proposed_secret_key_is_valid(secret_key):
        log("key did not match researcher")
        return abort(403)  # incorrect secret key
    
    api_key.update_only(last_used=timezone.now())
    return researcher


################################# Interact with the request ########################################
"""
In general use the decorators. These functions are the underlying components of those decorators,
they are complex and easy to misuse.
"""


def api_check_researcher_study_access(request: ResearcherRequest) -> Tuple[Study, Researcher]:
    """ Checks whether the researcher is allowed to do api access on this study. """
    # these two function cause aborts if they fail, this function exists to bundle them together
    # without side effects.
    study = api_get_study_confirm_exists(request)
    researcher = api_get_validate_researcher_on_study(request, study)
    return study, researcher


def api_get_and_sanitize_credentials(request: HttpRequest) -> Tuple[str, str]:
    """ Sanitize access and secret keys from request """
    try:
        access_key = request.POST.get("access_key", None)
        secret_key = request.POST.get("secret_key", None)
    except UnreadablePostError:
        log("unreadable post 1")
        return abort(500)
    
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


def api_get_validate_researcher_on_study(request: ResearcherRequest, study: Study) -> Researcher:
    """ Finds researcher based on the secret key provided.
    
    Returns 403 if researcher doesn't exist, is not credentialed on the study, or if the secret key
    does not match. """
    researcher = api_get_and_validate_researcher(request)
    
    # case site admins have access to everything.
    if researcher.site_admin:
        log("researcher is site_admin")
        return researcher
    
    # if the researcher has no relation to the study, 403.
    # case: researcher is not credentialed for this study.
    exists = StudyRelation.determine_relationship_exists(study_pk=study.pk, researcher_pk=researcher.pk)
    if not exists:
        log("no study relation found")
        return abort(403)
    return researcher


def api_get_study_confirm_exists(request: ResearcherRequest) -> Study:
    """
    Checks for a valid study object id or primary key.
    Study object id malformed (not 24 characters) causes 400 error.
    Study does not exist in our database causes 404 error.
    """
    try:
        study_object_id = request.POST.get('study_id', None)
        study_pk = request.POST.get('study_pk', None)
    except UnreadablePostError:
        log("unreadable post 2")
        return abort(500)
    
    if study_object_id is not None:
        # If the ID is incorrectly sized, we return a 400
        if not is_object_id(study_object_id):
            log("bad study obj id: ", study_object_id)
            return abort(400)
        
        # If no Study with the given ID exists, we return a 404
        try:
            return Study.objects.get(object_id=study_object_id, deleted=False)
        except Study.DoesNotExist:
            log(f"study '{study_object_id}' does not exist (obj id)")
            return abort(404)
    
    elif study_pk is not None:
        # study pk must coerce to an int
        try:
            int(study_pk)
        except ValueError:
            log("bad study pk")
            return abort(400)
        
        # If no Study with the given ID exists, we return a 404
        try:
            return Study.objects.get(pk=study_pk, deleted=False)
        except Study.DoesNotExist:
            log("study '%s' does not exist (study pk)" % study_object_id)
            return abort(404)
    
    else:
        log("no study provided at all")
        return abort(400)
