from django.contrib import messages
from django.core.exceptions import ValidationError
from django.shortcuts import redirect, render
from django.views.decorators.http import require_POST
from markupsafe import Markup

from authentication.admin_authentication import authenticate_admin
from constants.celery_constants import (ANDROID_FIREBASE_CREDENTIALS, BACKEND_FIREBASE_CREDENTIALS,
    IOS_FIREBASE_CREDENTIALS)
from constants.message_strings import (ALERT_ANDROID_DELETED_TEXT, ALERT_ANDROID_SUCCESS_TEXT,
    ALERT_ANDROID_VALIDATION_FAILED_TEXT, ALERT_DECODE_ERROR_TEXT, ALERT_EMPTY_TEXT,
    ALERT_FIREBASE_DELETED_TEXT, ALERT_IOS_DELETED_TEXT, ALERT_IOS_SUCCESS_TEXT,
    ALERT_IOS_VALIDATION_FAILED_TEXT, ALERT_MISC_ERROR_TEXT, ALERT_SPECIFIC_ERROR_TEXT,
    ALERT_SUCCESS_TEXT)
from database.system_models import FileAsText
from libs.endpoint_helpers.system_admin_helpers import (validate_android_credentials,
    validate_ios_credentials)
from libs.firebase_config import get_firebase_credential_errors, update_firebase_instance
from libs.internal_types import ResearcherRequest


########################## FIREBASE CREDENTIALS ENDPOINTS ##################################
# note: all of the strings passed in the following function (eg: ALERT_DECODE_ERROR_TEXT) are plain
# strings not intended for use with .format or other potential injection vectors

@authenticate_admin
def manage_firebase_credentials(request: ResearcherRequest):
    return render(
        request,
        'manage_firebase_credentials.html',
        dict(
            firebase_credentials_exists=FileAsText.objects.filter(tag=BACKEND_FIREBASE_CREDENTIALS).exists(),
            android_credentials_exists=FileAsText.objects.filter(tag=ANDROID_FIREBASE_CREDENTIALS).exists(),
            ios_credentials_exists=FileAsText.objects.filter(tag=IOS_FIREBASE_CREDENTIALS).exists(),
        )
    )


@require_POST
@authenticate_admin
def upload_backend_firebase_cert(request: ResearcherRequest):
    uploaded = request.FILES.get('backend_firebase_cert', None)
    
    if uploaded is None:
        messages.error(request, Markup(ALERT_EMPTY_TEXT))
        return redirect('/manage_firebase_credentials')
    
    try:
        cert = uploaded.read().decode()
    except UnicodeDecodeError:  # raised for an unexpected file type
        messages.error(request, Markup(ALERT_DECODE_ERROR_TEXT))
        return redirect('/manage_firebase_credentials')
    
    if not cert:
        messages.error(request, Markup(ALERT_EMPTY_TEXT))
        return redirect('/manage_firebase_credentials')
    
    instantiation_errors = get_firebase_credential_errors(cert)
    if instantiation_errors:
        # noinspection StrFormat
        # This string is sourced purely from the error message of get_firebase_credential_errors,
        # all of which are known-safe text. (no javascript injection)
        error_string = ALERT_SPECIFIC_ERROR_TEXT.format(error_message=instantiation_errors)
        messages.error(request, Markup(error_string))
        return redirect('/manage_firebase_credentials')
    
    # delete and recreate to get metadata timestamps
    FileAsText.objects.filter(tag=BACKEND_FIREBASE_CREDENTIALS).delete()
    FileAsText.objects.create(tag=BACKEND_FIREBASE_CREDENTIALS, text=cert)
    update_firebase_instance()
    messages.info(request, Markup(ALERT_SUCCESS_TEXT))
    return redirect('/manage_firebase_credentials')


@require_POST
@authenticate_admin
def upload_android_firebase_cert(request: ResearcherRequest):
    uploaded = request.FILES.get('android_firebase_cert', None)
    try:
        if uploaded is None:
            raise AssertionError("file name missing from upload")
        cert = uploaded.read().decode()
        if not cert:
            raise AssertionError("unexpected empty string")
        if not validate_android_credentials(cert):
            raise ValidationError('wrong keys for android cert')
        FileAsText.objects.get_or_create(tag=ANDROID_FIREBASE_CREDENTIALS, defaults={"text": cert})
        messages.info(request, Markup(ALERT_ANDROID_SUCCESS_TEXT))
    except AssertionError:
        messages.error(request, Markup(ALERT_EMPTY_TEXT))
    except UnicodeDecodeError:  # raised for an unexpected file type
        messages.error(request, Markup(ALERT_DECODE_ERROR_TEXT))
    except ValidationError:
        messages.error(request, Markup(ALERT_ANDROID_VALIDATION_FAILED_TEXT))
    except AttributeError:  # raised for a missing file
        messages.error(request, Markup(ALERT_EMPTY_TEXT))
    except ValueError:
        messages.error(request, Markup(ALERT_MISC_ERROR_TEXT))
    return redirect('/manage_firebase_credentials')


@require_POST
@authenticate_admin
def upload_ios_firebase_cert(request: ResearcherRequest):
    uploaded = request.FILES.get('ios_firebase_cert', None)
    try:
        if uploaded is None:
            raise AssertionError("file name missing from upload")
        cert = uploaded.read().decode()
        if not cert:
            raise AssertionError("unexpected empty string")
        if not validate_ios_credentials(cert):
            raise ValidationError('wrong keys for ios cert')
        FileAsText.objects.get_or_create(tag=IOS_FIREBASE_CREDENTIALS, defaults={"text": cert})
        messages.info(request, Markup(ALERT_IOS_SUCCESS_TEXT))
    except AssertionError:
        messages.error(request, Markup(ALERT_EMPTY_TEXT))
    except UnicodeDecodeError:  # raised for an unexpected file type
        messages.error(request, Markup(ALERT_DECODE_ERROR_TEXT))
    except AttributeError:  # raised for a missing file
        messages.error(request, Markup(ALERT_EMPTY_TEXT))
    except ValidationError:
        messages.error(request, Markup(ALERT_IOS_VALIDATION_FAILED_TEXT))
    except ValueError:
        messages.error(request, Markup(ALERT_MISC_ERROR_TEXT))
    return redirect('/manage_firebase_credentials')


@require_POST
@authenticate_admin
def delete_backend_firebase_cert(request: ResearcherRequest):
    FileAsText.objects.filter(tag=BACKEND_FIREBASE_CREDENTIALS).delete()
    # deletes the existing firebase app connection to clear credentials from memory
    update_firebase_instance()
    messages.info(request, Markup(ALERT_FIREBASE_DELETED_TEXT))
    return redirect('/manage_firebase_credentials')


@require_POST
@authenticate_admin
def delete_android_firebase_cert(request: ResearcherRequest):
    FileAsText.objects.filter(tag=ANDROID_FIREBASE_CREDENTIALS).delete()
    messages.info(request, Markup(ALERT_ANDROID_DELETED_TEXT))
    return redirect('/manage_firebase_credentials')


@require_POST
@authenticate_admin
def delete_ios_firebase_cert(request: ResearcherRequest):
    FileAsText.objects.filter(tag=IOS_FIREBASE_CREDENTIALS).delete()
    messages.info(request, Markup(ALERT_IOS_DELETED_TEXT))
    return redirect('/manage_firebase_credentials')
