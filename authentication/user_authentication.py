import functools

from flask import abort, request
from werkzeug.datastructures import MultiDict

from database.user_models import Participant


def get_session_participant() -> Participant:
    """ Safely and appropriately grabs the participant based on the structure of the request,
    which should be universal. First check is of the cache, which is also populated in the
    authentication wrapper code later in this file. """

    try:
        return request._beiwe_participant
    except AttributeError:
        pass

    try:
        participant_id = request.values['patient_id']
    except KeyError:
        return abort(400)  # invalid post request structure

    try:
        participant = Participant.objects.get(patient_id=participant_id)
    except Participant.DoesNotExist:
        return abort(404)  # invalid participant id

    request._beiwe_participant = participant

    return participant


####################################################################################################


def minimal_validation(some_function) -> callable:
    @functools.wraps(some_function)
    def authenticate_and_call(*args, **kwargs):
        is_ios = kwargs.get("OS_API", None) == Participant.IOS_API
        correct_for_basic_auth()
        if validate_post_ignore_password(is_ios):
            return some_function(*args, **kwargs)
        return abort(401 if is_ios else 403)
    return authenticate_and_call


def validate_post_ignore_password(is_ios) -> bool:
    """Check if user exists, that a password was provided but ignores its validation, and if the
    device id matches.
    IOS apparently has problems retaining the device id, so we want to bypass it when it is an ios user
    """
    rv = request.values
    if "patient_id" not in rv or "password" not in rv or "device_id" not in rv:
        return False

    participant_set = Participant.objects.filter(patient_id=request.values['patient_id'])
    if not participant_set.exists():
        return False
    participant = participant_set.get()

    # Disabled
    # if not participant.validate_password(request.values['password']):
    #     return False
    # Only execute if it is an android device
    # if not is_ios and not participant.device_id == request.values['device_id']:
    #     return False

    request._beiwe_participant = participant  # cache participant
    return True

####################################################################################################


def authenticate_user(some_function) -> callable:
    """Decorator for functions (pages) that require a user to provide identification. Returns 403
    (forbidden) or 401 (depending on beiwei-api-version) if the identifying info (usernames,
    passwords device IDs are invalid.

   In any funcion wrapped with this decorator provide a parameter named "patient_id" (with the
   user's id), a parameter named "password" with an SHA256 hashed instance of the user's
   password, a parameter named "device_id" with a unique identifier derived from that device. """
    @functools.wraps(some_function)
    def authenticate_and_call(*args, **kwargs):
        correct_for_basic_auth()
        if validate_post():
            return some_function(*args, **kwargs)
        return abort(401 if (kwargs.get("OS_API", None) == Participant.IOS_API) else 403)
    return authenticate_and_call


def validate_post() -> bool:
    """Check if user exists, check if the provided passwords match, and if the device id matches."""
    rv = request.values
    if "patient_id" not in rv or "password" not in rv or "device_id" not in rv:
        return False

    participant_set = Participant.objects.filter(patient_id=request.values['patient_id'])
    if not participant_set.exists():
        return False
    participant = participant_set.get()
    if not participant.validate_password(request.values['password']):
        return False
    if not participant.device_id == request.values['device_id']:
        return False

    request._beiwe_participant = participant  # cache participant
    return True


def authenticate_user_registration(some_function) -> callable:
    """ Decorator for functions (pages) that require a user to provide identification. Returns
    403 (forbidden) or 401 (depending on beiwe-api-version) if the identifying info (username,
    password, device ID) are invalid.

   In any function wrapped with this decorator provide a parameter named "patient_id" (with the
   user's id) and a parameter named "password" with an SHA256 hashed instance of the user's
   password. """
    @functools.wraps(some_function)
    def authenticate_and_call(*args, **kwargs):
        correct_for_basic_auth()
        if validate_registration():
            return some_function(*args, **kwargs)
        return abort(401 if (kwargs.get("OS_API", None) == Participant.IOS_API) else 403)
    return authenticate_and_call


def validate_registration() -> bool:
    """Check if user exists, check if the provided passwords match"""
    rv = request.values
    if "patient_id" not in rv or "password" not in rv or "device_id" not in rv:
        return False

    participant_set = Participant.objects.filter(patient_id=request.values['patient_id'])
    if not participant_set.exists():
        return False
    participant = participant_set.get()
    if not participant.validate_password(request.values['password']):
        return False
    return True


# TODO: basic auth is not a good thing, it is only used because it was easy and we enforce
#  https on all connections.  Review.
def correct_for_basic_auth():
    """
    Basic auth is used in IOS.
    
    If basic authentication exists and is in the correct format, move the patient_id,
    device_id, and password into request.values for processing by the existing user
    authentication functions.
    
    Flask automatically parses a Basic authentication header into request.authorization
    
    If this is set, and the username portion is in the form xxxxxx@yyyyyyy, then assume this is
    patient_id@device_id.
    
    Parse out the patient_id, device_id from username, and then store patient_id, device_id and
    password as if they were passed as parameters (into request.values)
    
    Note:  Because request.values is immutable in Flask, copy it and replace with a mutable dict
    first.
    
    Check if user exists, check if the provided passwords match.
    """
    
    auth = request.authorization
    if not auth:
        return
    
    username_parts = auth.username.split('@')
    if len(username_parts) == 2:
        replace_dict = MultiDict(request.values.to_dict())
        if "patient_id" not in replace_dict:
            replace_dict['patient_id'] = username_parts[0]
        if "device_id" not in replace_dict:
            replace_dict['device_id'] = username_parts[1]
        if "password" not in replace_dict:
            replace_dict['password'] = auth.password
        request.values = replace_dict
