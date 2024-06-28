import functools
from datetime import datetime, timedelta
from typing import Optional

import bleach
from django.contrib import messages
from django.db.models import QuerySet
from django.http import UnreadablePostError
from django.http.request import HttpRequest
from django.shortcuts import HttpResponseRedirect, redirect
from django.utils import timezone
from django.utils.timezone import is_naive

from config.settings import REQUIRE_SITE_ADMIN_MFA
from constants.message_strings import (MFA_CONFIGURATION_REQUIRED, MFA_CONFIGURATION_SITE_ADMIN,
    PASSWORD_EXPIRED, PASSWORD_RESET_FORCED, PASSWORD_RESET_SITE_ADMIN, PASSWORD_RESET_TOO_SHORT,
    PASSWORD_WILL_EXPIRE)
from constants.url_constants import LOGIN_REDIRECT_IGNORE, LOGIN_REDIRECT_SAFE
from constants.user_constants import (ALL_RESEARCHER_TYPES, EXPIRY_NAME, ResearcherRole,
    SESSION_NAME, SESSION_TIMEOUT_HOURS, SESSION_UUID)
from database.study_models import Study
from database.user_models_researcher import Researcher, StudyRelation
from libs.http_utils import easy_url
from libs.internal_types import ResearcherRequest
from libs.password_validation import get_min_password_requirement
from libs.security import generate_easy_alphanumeric_string
from middleware.abort_middleware import abort


DEBUG_ADMIN_AUTHENTICATION = False


def log(*args, **kwargs):
    if DEBUG_ADMIN_AUTHENTICATION:
        print(*args, **kwargs)


# Top level authentication wrappers
def authenticate_researcher_login(some_function):
    """ Decorator for functions (pages) that require a login, redirect to login page on failure. """
    @functools.wraps(some_function)
    def authenticate_and_call(*args, **kwargs):
        # typecheck, this decorator assumes a django HttpRequest is the first parameter
        request: ResearcherRequest = args[0]
        assert isinstance(request, HttpRequest), \
            f"first parameter of {some_function.__name__} must be an HttpRequest, was {type(request)}."
        
        if check_is_logged_in(request):
            # log them in, determine any top-level redirects, otherwise continue to target function
            populate_session_researcher(request)
            goto_redirect = determine_any_redirects(request)
            return goto_redirect if goto_redirect else some_function(*args, **kwargs)
        else:
            return do_login_page_redirect(request)
    
    return authenticate_and_call


################################################################################
############################ Website Functions #################################
################################################################################


def logout_researcher(request: HttpRequest):
    """ clear session information for a researcher """
    if SESSION_UUID in request.session:
        del request.session[SESSION_UUID]
    if EXPIRY_NAME in request.session:
        del request.session[EXPIRY_NAME]
    if SESSION_NAME in request.session:
        del request.session[SESSION_NAME]


def log_in_researcher(request: ResearcherRequest, username: str):
    """ Populate session for a researcher - should only be called from  validate_login endpoint. """
    request.session[SESSION_UUID] = generate_easy_alphanumeric_string()
    request.session[EXPIRY_NAME] = timezone.now() + timedelta(hours=SESSION_TIMEOUT_HOURS)
    request.session[SESSION_NAME] = username
    Researcher.objects.filter(username=username).update(last_login_time=timezone.now())


def check_is_logged_in(request: ResearcherRequest):
    """ automatically log out the researcher if their session is timed out, extend session if the
    user is already logged in. """
    
    if EXPIRY_NAME not in request.session:
        # no session expiry present at all, user is not logged in.
        log("no session expiry present")
        return False
    
    if SESSION_UUID not in request.session:
        # no session uuid present at all, user is not logged in.
        log("no session key present")
        return False
    
    if handle_session_expiry(request):
        return True
    else:
        log("session had expired")
    logout_researcher(request)
    return False


def handle_session_expiry(request: ResearcherRequest):
    # Sometimes the datetime is naive, probably a development environment bug? We force a timezone
    # in updating the expiry later, this is a safety check.
    expiry_time: datetime = request.session[EXPIRY_NAME]
    now = datetime.now() if is_naive(expiry_time) else timezone.now()
    
    # session has expired, do not refresh - the returned-to code will call logout_researcher.
    if expiry_time < now:
        return False
    
    # If the session has (less than) 10 seconds until its timeout period, WE DON'T REFRESH IT. This
    # is how we implement a you-will-be-logged-out feature. A failure mode of 10 seconds where the
    # page works and then logs you out is.... fine. Its fine.
    if (expiry_time - now).total_seconds() < 10:
        return True
    
    # reset the session expiry to 2 hours from now - force timezone'd datetime.
    request.session[EXPIRY_NAME] = timezone.now() + timedelta(hours=SESSION_TIMEOUT_HOURS)
    return True


def populate_session_researcher(request: ResearcherRequest):
    # this function defines the ResearcherRequest, which is purely for IDE assistence
    username = request.session.get(SESSION_NAME, None)
    if username is None:
        log("researcher username was not present in session")
        return abort(400)
    try:
        # Cache the Researcher into request.session_researcher.
        request.session_researcher = Researcher.objects.get(username=username)
    except Researcher.DoesNotExist:
        log("could not identify researcher in session")
        return abort(400)


def assert_admin(request: ResearcherRequest, study_id: int):
    """ This function will throw a 403 forbidden error and stop execution.  Note that the abort
        directly raises the 403 error, if we don't hit that return True. """
    session_researcher = request.session_researcher
    if not session_researcher.site_admin and not session_researcher.check_study_admin(study_id):
        # messages.warning("This user does not have admin privilages on this study.")
        log("no admin privilages")
        return abort(403)
    
    # allow usage in if statements
    return True


def assert_site_admin(request: ResearcherRequest):
    """ This function will throw a 403 forbidden error and stop execution.  Note that the abort
        directly raises the 403 error, if we don't hit that return True. """
    if not request.session_researcher.site_admin:
        # messages.warning("This user is not a site administrator.")
        log("not a site admin")
        return abort(403)
    
    # allow usage in if statements
    return True


def assert_researcher_under_admin(request: ResearcherRequest, researcher: Researcher, study=None):
    """ Asserts that the researcher provided is allowed to be edited by the session user.
        If study is provided then the admin test is strictly for that study, otherwise it checks
        for admin status anywhere. """
    session_researcher = request.session_researcher
    if session_researcher.site_admin:
        log("site admin checking for researcher")
        return
    
    if researcher.site_admin:
        messages.warning(request, "This user is a site administrator, action rejected.")
        log("target researcher is a site admin")
        return abort(403)
    
    kwargs = dict(relationship=ResearcherRole.study_admin)
    if study is not None:
        kwargs['study'] = study
    
    if researcher.study_relations.filter(**kwargs).exists():
        messages.warning(request, "This user is a study administrator, action rejected.")
        log("target researcher is a study administrator")
        return abort(403)
    
    session_studies = set(session_researcher.get_admin_study_relations().values_list("study_id", flat=True))
    researcher_studies = set(researcher.get_researcher_study_relations().values_list("study_id", flat=True))
    
    if not session_studies.intersection(researcher_studies):
        messages.warning(request, "You are not an administrator for that researcher, action rejected.")
        log("session researcher is not an administrator of target researcher")
        return abort(403)


################################################################################
########################## Study Editing Privileges ############################
################################################################################

class ArgumentMissingException(Exception): pass


def authenticate_researcher_study_access(some_function):
    """ This authentication decorator checks whether the user has permission to to access the
    study/survey they are accessing.
    This decorator requires the specific keywords "survey_id" or "study_id" be provided as
    keywords to the function, and will error if one is not.
    The pattern is for a url with <string:survey/study_id> to pass in this value.
    A site admin is always able to access a study or survey. """
    
    @functools.wraps(some_function)
    def authenticate_and_call(*args, **kwargs):
        return authenticate_researcher_study_access_and_call(some_function, *args, **kwargs)
    
    return authenticate_and_call


# we need to be able to import this for a special case in the study_api.py file
def authenticate_researcher_study_access_and_call(some_function, *args, **kwargs):
    # Check for regular login requirement
    request: ResearcherRequest = args[0]
    assert isinstance(request, HttpRequest), \
        f"first parameter of {some_function.__name__} must be an HttpRequest, was {type(request)}."
    
    if not check_is_logged_in(request):
        log("researcher is not logged in")
        return do_login_page_redirect(request)
    
    populate_session_researcher(request)
    
    try:
        # first get from kwargs, then from the POST request, either one is fine
        survey_id = kwargs.get('survey_id', request.POST.get('survey_id', None))
        study_id = kwargs.get('study_id', request.POST.get('study_id', None))
    except UnreadablePostError:
        log("unreadable post error")
        return abort(500)
    
    # Check proper usage
    if survey_id is None and study_id is None:
        log("no survey or study provided")
        return abort(400)
    
    if survey_id is not None and study_id is None:
        log("survey was provided but no study was provided")
        return abort(400)
    
    # TODO REAL TEST FOR THIS
    try:
        if survey_id is not None:
            survey_id = int(survey_id)
        if study_id is not None:
            study_id = int(study_id)
    except ValueError:
        log("survey or study id was not an integer")
        return abort(400)
    
    # We want the survey_id check to execute first if both args are supplied, surveys are
    # attached to studies but do not supply the study id.
    if survey_id:
        # get studies for a survey, fail with 404 if study does not exist
        studies = Study.objects.filter(surveys=survey_id)
        if not studies.exists():
            log("no such study 1")
            return abort(404)
        
        # check that the study found matches the study id.
        if study_id != studies.values_list('pk', flat=True).get():
            log("study id mismatch")
            return abort(404)
    
    # assert that such a study exists
    if not Study.objects.filter(pk=study_id, deleted=False).exists():
        log("no such study 2")
        return abort(404)
    
    # always allow site admins, allow all types of study relations
    if not request.session_researcher.site_admin:
        try:
            relation = StudyRelation.objects \
                .filter(study_id=study_id, researcher=request.session_researcher) \
                .values_list("relationship", flat=True).get()
        except StudyRelation.DoesNotExist:
            log("no study relationship for researcher")
            return abort(403)
        
        if relation not in ALL_RESEARCHER_TYPES:
            log("invalid study relationship for researcher")
            return abort(403)
    
    goto_redirect = determine_any_redirects(request)
    return goto_redirect if goto_redirect else some_function(*args, **kwargs)


def get_researcher_allowed_studies_as_query_set(request: ResearcherRequest) -> QuerySet[Study]:
    if request.session_researcher.site_admin:
        return Study.get_all_studies_by_name()
    
    return Study.get_all_studies_by_name().filter(
        id__in=request.session_researcher.study_relations.values_list("study", flat=True)
    )


################################################################################
############################# Site Administrator ###############################
################################################################################

def authenticate_admin(some_function):
    """ Authenticate site admin, checks whether a user is a system admin before allowing access to
    pages marked with this decorator.  If a study_id variable is supplied as a keyword argument, the
    decorator will automatically grab the ObjectId in place of the string provided in a route.

    NOTE: if you are using this function along with the authenticate_researcher_study_access
    decorator you must place this decorator below it, otherwise behavior is undefined and probably
    causes a 500 error inside the authenticate_researcher_study_access decorator. """
    @functools.wraps(some_function)
    def authenticate_and_call(*args, **kwargs):
        request: ResearcherRequest = args[0]
        
        # this is debugging code for the django frontend server port
        if not isinstance(request, HttpRequest):
            raise TypeError(f"request was a {type(request)}, expected {HttpRequest}")
        
        # Check for regular login requirement
        if not check_is_logged_in(request):
            return do_login_page_redirect(request)
        
        populate_session_researcher(request)
        session_researcher = request.session_researcher
        # if researcher is not a site admin assert that they are a study admin somewhere, then test
        # the special case of a the study id, if it is present.
        if not session_researcher.site_admin:
            if not session_researcher.study_relations.filter(relationship=ResearcherRole.study_admin).exists():
                log("not study admin anywhere")
                return abort(403)
            
            # fail if there is a study_id and it either does not exist or the researcher is not an
            # admin on that study.
            if 'study_id' in kwargs:
                if not StudyRelation.objects.filter(
                    researcher=session_researcher,
                    study_id=kwargs['study_id'],
                    relationship=ResearcherRole.study_admin,
                ).exists():
                    log("not study admin on study")
                    return abort(403)
        
        # validate the study_id if it is present
        if 'study_id' in kwargs:
            if not Study.objects.filter(id=kwargs['study_id']).exists():
                log("no such study")
                return abort(404)
        
        # determine whether to redirect to password the password reset page
        goto_redirect = determine_any_redirects(request)
        return goto_redirect if goto_redirect else some_function(*args, **kwargs)
    
    return authenticate_and_call


def forest_enabled(func):
    """ Decorator for validating that Forest is enabled for this study. """
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            study = Study.objects.get(id=kwargs.get("study_id", None))
        except Study.DoesNotExist:
            return abort(404)
        
        if not study.forest_enabled:
            return abort(404)
        
        return func(*args, **kwargs)
    
    return wrapped


############################################################################
############################# Redirect Logic ###############################
############################################################################


def determine_any_redirects(request: ResearcherRequest) -> Optional[HttpResponseRedirect]:
    """ Check for all Researcher user states where the session needs to be redirected.  This
    function is called on every page load. Currently includes password reset and password expiry."""
    researcher = request.session_researcher
    matchable_url_path = request.get_full_path().lstrip("/")  # have to remove the slash.
    
    # case: the password reset page could otherwise be infinitely redirected to itself;
    # need to allow user to log out, to set a new password, and be able to reset mfa. Currently
    # this means we block the manage_credentials page from getting a login-page-style redirect...
    for url_pattern in LOGIN_REDIRECT_IGNORE:
        if url_pattern.pattern.match(matchable_url_path):
            # print("ignoring all redirect logic for", request.get_full_path())
            return None
    
    # if the researcher has a forced password reset, or the researcher is on a study that requires
    # a minimum password length, force them to the reset password page.
    if researcher.password_force_reset:
        log("password reset forced")
        messages.error(request, PASSWORD_RESET_FORCED)
        return redirect(easy_url("admin_pages.manage_credentials"))
    
    if researcher.password_min_length < get_min_password_requirement(researcher):
        log("password reset min length")
        messages.error(
            request, PASSWORD_RESET_SITE_ADMIN if researcher.site_admin else PASSWORD_RESET_TOO_SHORT
        )
        return redirect(easy_url("admin_pages.manage_credentials"))
    
    # get smallest password max age from studies the researcher is on
    max_age_days = researcher.study_relations \
                       .filter(study__password_max_age_enabled=True) \
                       .order_by("study__password_max_age_days") \
                       .values_list("study__password_max_age_days", flat=True) \
                       .first()
    
    if max_age_days:
        log("any password reset checks")
        # determine the age of the password, if it is within 7 days of expiring warn the user,
        # if it is expired force them to the reset password page.
        password_age_days = (timezone.now() - researcher.password_last_changed).days
        if password_age_days > max_age_days:
            messages.error(request, PASSWORD_EXPIRED)
            log("password expired, redirecting")
            return redirect(easy_url("admin_pages.manage_credentials"))
        elif password_age_days > max_age_days - 7:
            messages.warning(request, PASSWORD_WILL_EXPIRE.format(days=max_age_days - password_age_days))
    
    # based on the researcher's studies (or if they are a site admin, based on the
    # REQUIRE_SITE_ADMIN_MFA setting) determine if MFA is required. Force researchers without MFA to
    # redirect to the manage credentials page with a message.
    if researcher.requires_mfa and not researcher.mfa_token:
        messages.error(request, MFA_CONFIGURATION_REQUIRED)
        if researcher.site_admin and REQUIRE_SITE_ADMIN_MFA:
            messages.error(request, MFA_CONFIGURATION_SITE_ADMIN)
        return redirect(easy_url("admin_pages.manage_credentials"))
    
    # request.get_full_path() is always a valid url because this code only executes on pages that
    # were already validly resolved. (we need the slash at the beginning when storing to the db)
    for url_pattern in LOGIN_REDIRECT_SAFE:
        if url_pattern.pattern.match(matchable_url_path):
            researcher.update_only(most_recent_page=request.get_full_path())
            break


def do_login_page_redirect(request: HttpRequest) -> HttpResponseRedirect:
    """ The login page can have a parameter appended to the url to redirect to a specific page. This
    parameter is injected based the http request referrer (which is validated!). This allows someone
    to click on a link, get redirected to the login page, log in, and then they get directed to the
    page they clicked the link for. """
    referrer_url = request.get_full_path().lstrip("/")
    # this should be overkill: ensure that the url is url-safe (because all our urls are url-safe)
    # and only then append that to the redirect url as a get parameter.
    if bleach.clean(referrer_url) == referrer_url and determine_redirectable(referrer_url):
        return redirect("/" + "?page=/" + referrer_url)
    return redirect("/")


def determine_redirectable(redirect_page_url: str) -> bool:
    """ Runs a url string through the list of redirectable urls looking for pattern matches, returns
    True if there are any matches. """
    # matches only work if there is no leading slash.
    matchable_redirect_page = redirect_page_url.lstrip("/")
    for url_pattern in LOGIN_REDIRECT_SAFE:
        if url_pattern.pattern == matchable_redirect_page or url_pattern.pattern.match(matchable_redirect_page):
            return True
    return False
