import functools

import orjson
from django.http import UnreadablePostError
from django.http.request import HttpRequest

from constants.user_constants import IOS_API
from database.user_models_participant import Participant, SurveyNotificationReport
from libs.internal_types import ParticipantRequest
from libs.sentry import elastic_beanstalk_error_sentry
from middleware.abort_middleware import abort


DEBUG_PARTICIPANT_AUTHENTICATION = False


def log(*args, **kwargs):
    if DEBUG_PARTICIPANT_AUTHENTICATION:
        print("PARTICIPANT AUTH:", *args, **kwargs)


def validate_post(request: HttpRequest, require_password: bool, registration: bool) -> bool:
    """Check if user exists, check if the provided passwords match, and if the device id matches."""
    # even if the password won't be checked we want the key to be present.
    try:
        post_data = request.POST
    except UnreadablePostError:
        return abort(500)
    
    if "patient_id" not in post_data or "password" not in post_data or "device_id" not in post_data:
        log("missing parameters entirely.")
        log("patient_id:", "patient_id" in post_data)
        log("password:", "password" in post_data)
        log("device_id:", "device_id" in post_data)
        return False
    log("all parameters present...")
    
    # FIXME: Device Testing. need to check the app expectations on response codes
    #  this used to throw a 400 if the there was no patient_id field in the post request,
    #  and 404 when there was no such user, when it was get_session_participant.
    # This isn't True? the old code included the test for presence of keys, and returned False,
    #  triggering the os-specific failure codes.
    try:
        session_participant: Participant = \
            Participant.objects.get(patient_id=request.POST['patient_id'])
    except Participant.DoesNotExist:
        log("invalid patient_id")
        return False
    except UnreadablePostError:
        return abort(500)
    
    if session_participant.is_dead:
        log("dead participant")
        return False
    
    # request.POST['device_id'] is a string, session_participant.device_id will eventually be a uuid
    device_id = request.POST['device_id']
    if session_participant.device_id != device_id:
        if not device_id:
            # this should not happen ever. If it does it is a bug in the app.
            raise Exception("device_id was empty in a request to the server.")
        session_participant.update_only(device_id=device_id)
    
    # check participants and studies for easy enrollment
    if registration:
        if session_participant.easy_enrollment or session_participant.study.easy_enrollment:
            require_password = False
    
    try:
        if require_password:
            if not session_participant.validate_password(request.POST['password']):
                log("incorrect password")
                return False
            log("password passes validation")
        else:
            log("password validation skipped")
    except UnreadablePostError:
        return abort(500)
    
    run_participant_db_updates(request, session_participant)
    
    # attach session participant to request object, defining the ParticipantRequest class.
    request.session_participant = session_participant
    return True


def run_participant_db_updates(request: HttpRequest, participant: Participant):
    """ Single function for all database mutations based on content of incoming requests.
    This emits multiple database updates, but at time of commenting, sept 2024, we just don't really
    care about how long this takes to run. """
    
    ## App and OS version tracking 
    # get the existing version code info
    prior_version_code = participant.last_version_code
    prior_version_name = participant.last_version_name
    prior_os_version = participant.last_os_version
    
    # old versions of the apps do not report these values
    tracking_updates = {}
    if "version_code" in request.POST:
        tracking_updates['last_version_code'] = request.POST["version_code"][:32]
    if "version_name" in request.POST:
        tracking_updates['last_version_name'] = request.POST["version_name"][:32]
    if "os_version" in request.POST:
        tracking_updates['last_os_version'] = request.POST["os_version"][:32]
    if "device_status_report" in request.POST:
        tracking_updates['device_status_report'] = request.POST["device_status_report"]
    if tracking_updates:
        participant.update_only(**tracking_updates)
    
    # attribute is updated in update_only
    if (prior_version_code != participant.last_version_code or
        prior_version_name != participant.last_version_name or
        prior_os_version != participant.last_os_version):
        # log(f"os version changed: {last_version_code} to {session_participant.last_version_code}")
        participant.generate_app_version_history(
            prior_version_code, prior_version_name, prior_os_version
        )
    
    # we generate a log of the device status report, we do compress the data tho.
    if participant.enable_extensive_device_info_tracking:
        participant.generate_device_status_report_history(request.path_info)
    
    # updating the timezone is a special case, has internal logic.
    if "timezone" in request.POST:
        # protect against problematic inputs
        if request.POST["timezone"] is None or request.POST["timezone"] != "":
            participant.try_set_timezone(request.POST["timezone"])
    
    if "survey_uuids" in request.POST:
        # uuids are a json list of strings, we only allow strings through.
        uuids = request.POST["survey_uuids", "[]"]
        uuids = [uuid for uuid in orjson.loads(uuids) if isinstance(uuid, str)]
        if not isinstance(uuids, list):
            uuids = []
            with elastic_beanstalk_error_sentry():
                raise TypeError(f"Device survey_uuids should be a list, found instead: {type(uuids)}.")
        
        if uuids:
            potentially_new_uuids = [
                SurveyNotificationReport(participant=participant, notification_uuid=a_uuid)
                for a_uuid in uuids
            ]
            SurveyNotificationReport.objects.bulk_create(potentially_new_uuids, ignore_conflicts=True)


####################################################################################################


def minimal_validation(some_function) -> callable:
    
    @functools.wraps(some_function)
    def authenticate_and_call(*args, **kwargs):
        request: ParticipantRequest = args[0]
        assert isinstance(request, HttpRequest), \
            f"first parameter of {some_function.__name__} must be an HttpRequest, was {type(request)}."
        correct_for_basic_auth(request)
        
        if validate_post(request, require_password=False, registration=False):
            return some_function(*args, **kwargs)
        
        # ios requires different http codes
        is_ios = kwargs.get("OS_API", None) == IOS_API
        return abort(401 if is_ios else 403)
    
    return authenticate_and_call


def authenticate_participant(some_function) -> callable:
    """Decorator for functions (pages) that require a user to provide identification. Returns 403
    (forbidden) or 401 (depending on beiwei-api-version) if the identifying info (usernames,
    passwords device IDs are invalid.

    In any funcion wrapped with this decorator provide a parameter named "patient_id" (with the
    user's id), a parameter named "password" with an SHA256 hashed instance of the user's
    password, a parameter named "device_id" with a unique identifier derived from that device. """
    
    @functools.wraps(some_function)
    def authenticate_and_call(*args, **kwargs):
        request: ParticipantRequest = args[0]
        assert isinstance(request, HttpRequest), \
            f"first parameter of {some_function.__name__} must be an HttpRequest, was {type(request)}."
        correct_for_basic_auth(request)
        
        if validate_post(request, require_password=True, registration=False):
            return some_function(*args, **kwargs)
        is_ios = kwargs.get("OS_API", None) == IOS_API
        return abort(401 if is_ios else 403)
    
    return authenticate_and_call


def authenticate_participant_registration(some_function) -> callable:
    """ Decorator for functions (pages) that require a user to provide identification. Returns
    403 (forbidden) or 401 (depending on beiwe-api-version) if the identifying info (username,
    password, device ID) are invalid.
    
    In any function wrapped with this decorator provide a parameter named "patient_id" (with the
    user's id) and a parameter named "password" with an SHA256 hashed instance of the user's
    password. """
    
    @functools.wraps(some_function)
    def authenticate_and_call(*args, **kwargs):
        request: ParticipantRequest = args[0]
        assert isinstance(request, HttpRequest), \
            f"first parameter of {some_function.__name__} must be an HttpRequest, was {type(request)}."
        correct_for_basic_auth(request)
        
        if validate_post(request, require_password=True, registration=True):
            return some_function(*args, **kwargs)
        
        is_ios = kwargs.get("OS_API", None) == IOS_API
        return abort(401 if is_ios else 403)
    
    return authenticate_and_call


# TODO: basic auth is not a good thing, it is only used because it was easy and we enforce
#  https on all connections.  Fundamentally we need a rewrite of the participant auth structure to
#  disconnect it from the user password.  This is a major undertaking.
def correct_for_basic_auth(request: ParticipantRequest):
    """ Basic auth is used in IOS.
    If basic authentication exists and is in the correct format, move the patient_id, device_id, and
    password into request.values for processing by the existing user authentication functions.
    
    Django  parses a Basic authentication header into request.META
    
    If this is set, and the username portion is in the form xxxxxx@yyyyyyy, then assume this is
    patient_id@device_id. Parse out the patient_id, device_id from username, and then store
    patient_id, device_id and password as if they were passed as parameters (into request.POST) """
    
    if 'HTTP_AUTHORIZATION' in request.META:
        auth = request.META['HTTP_AUTHORIZATION'].split()
        if len(auth) != 2:
            raise Exception(f"incorrect basic auth length: {str(auth)}")
        
        if not auth[0].lower() == "basic":
            raise Exception(f"wrong basic auth format: {str(auth)}")
        
        username_parts, password = auth[1].split(':')
        patient_id, device_id = username_parts.split('@')
        
        try:
            request.POST['patient_id'] = patient_id
            request.POST['device_id'] = device_id
            request.POST['password'] = password
        except UnreadablePostError:
            return abort(500)
