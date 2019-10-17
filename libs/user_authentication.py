import functools

from flask import request, abort
from werkzeug.datastructures import MultiDict

from database.user_models import Participant


####################################################################################################


def minimal_validation(some_function):
    @functools.wraps(some_function)
    def authenticate_and_call(*args, **kwargs):
        is_ios = kwargs["OS_API"] == Participant.IOS_API
        correct_for_basic_auth()
        if validate_post_ignore_password(is_ios):
            return some_function(*args, **kwargs)
        return abort(401 if (kwargs["OS_API"] == Participant.IOS_API) else 403)
    return authenticate_and_call


def validate_post_ignore_password(is_ios):
    """Check if user exists, that a password was provided but ignores its validation, and if the
    device id matches.
    IOS apparently has problems retaining the device id, so we wantt to bypass it when it is an ios user
    """
    if ("patient_id" not in request.values
        or "password" not in request.values
        or "device_id" not in request.values):
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
    #     print "device_id is wrong"
    #     return False
    return True

####################################################################################################


def authenticate_user(some_function):
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
        return abort(401 if (kwargs["OS_API"] == Participant.IOS_API) else 403)
    return authenticate_and_call


def validate_post():
    """Check if user exists, check if the provided passwords match, and if the device id matches."""
    # print "user info:  ", request.values.items()
    # print "file info:  ", request.files.items()
    if ("patient_id" not in request.values
            or "password" not in request.values
            or "device_id" not in request.values):
        return False
    participant_set = Participant.objects.filter(patient_id=request.values['patient_id'])
    if not participant_set.exists():
        return False
    participant = participant_set.get()
    if not participant.validate_password(request.values['password']):
        return False
    if not participant.device_id == request.values['device_id']:
        return False
    return True


def authenticate_user_registration(some_function):
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
        return abort(401 if (kwargs["OS_API"] == Participant.IOS_API) else 403)
    return authenticate_and_call


def validate_registration():
    """Check if user exists, check if the provided passwords match"""
    if ("patient_id" not in request.values
            or "password" not in request.values
            or "device_id" not in request.values):
        return False
    participant_set = Participant.objects.filter(patient_id=request.values['patient_id'])
    if not participant_set.exists():
        return False
    participant = participant_set.get()
    if not participant.validate_password(request.values['password']):
        return False
    return True


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
    return
