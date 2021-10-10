import json
import plistlib
from collections import defaultdict

from django.core.exceptions import ValidationError
from flask import abort, Blueprint, escape, flash, Markup, redirect, render_template, request

from authentication.admin_authentication import (assert_admin, assert_researcher_under_admin,
    authenticate_admin, authenticate_researcher_study_access, get_researcher_allowed_studies,
    get_session_researcher, researcher_is_an_admin)
from config.constants import (ANDROID_FIREBASE_CREDENTIALS, BACKEND_FIREBASE_CREDENTIALS,
    CHECKBOX_TOGGLES, IOS_FIREBASE_CREDENTIALS, ResearcherRole, TIMER_VALUES)
from database.study_models import Study
from database.survey_models import Survey
from database.system_models import FileAsText
from database.user_models import Researcher, StudyRelation
from libs.copy_study import copy_study_from_json, format_study, unpack_json_study
from libs.firebase_config import get_firebase_credential_errors, update_firebase_instance
from libs.http_utils import checkbox_to_boolean, string_to_int
from libs.sentry import make_error_sentry, SentryTypes
from libs.timezone_dropdown import ALL_TIMEZONES_DROPDOWN
from pages.message_strings import (ALERT_ANDROID_DELETED_TEXT, ALERT_ANDROID_SUCCESS_TEXT,
    ALERT_ANDROID_VALIDATION_FAILED_TEXT, ALERT_DECODE_ERROR_TEXT, ALERT_EMPTY_TEXT,
    ALERT_FIREBASE_DELETED_TEXT, ALERT_IOS_DELETED_TEXT, ALERT_IOS_SUCCESS_TEXT,
    ALERT_IOS_VALIDATION_FAILED_TEXT, ALERT_MISC_ERROR_TEXT, ALERT_SPECIFIC_ERROR_TEXT,
    ALERT_SUCCESS_TEXT)


system_admin_pages = Blueprint('system_admin_pages', __name__)
SITE_ADMIN = "Site Admin"


@system_admin_pages.context_processor
def inject_html_params():
    # these variables will be accessible to every template rendering attached to the blueprint
    return {
        "allowed_studies": get_researcher_allowed_studies(),
        "is_admin": researcher_is_an_admin(),
        "session_researcher": get_session_researcher(),
    }


####################################################################################################
###################################### Helpers #####################################################
####################################################################################################

def get_administerable_studies_by_name():
    """ Site admins see all studies, study admins see only studies they are admins on. """
    researcher_admin = get_session_researcher()
    if researcher_admin.site_admin:
        studies = Study.get_all_studies_by_name()
    else:
        studies = researcher_admin.get_administered_studies_by_name()
    return studies


def get_administerable_researchers():
    """ Site admins see all researchers, study admins see researchers on their studies. """
    researcher_admin = get_session_researcher()
    if researcher_admin.site_admin:
        relevant_researchers = Researcher.filter_alphabetical()
    else:
        relevant_researchers = researcher_admin.get_administered_researchers_by_username()
    return relevant_researchers


def unflatten_consent_sections(consent_sections_dict):
    # consent_sections is a flat structure with structure like this:
    # { 'label_ending_in.text': 'text content',  'label_ending_in.more': 'more content' }
    # we need to transform it into a nested structure like this:
    # { 'label': {'text':'text content',  'more':'more content' }
    refactored_consent_sections = defaultdict(dict)
    for key, content in consent_sections_dict.items():
        _, label, content_type = key.split(".")
        refactored_consent_sections[label][content_type] = content
    return dict(refactored_consent_sections)


def get_session_researcher_study_ids():
    """ Returns the appropriate study ids based on whether a user is a study or site admin """
    session_researcher = get_session_researcher()
    if session_researcher.site_admin:
        return Study.objects.exclude(deleted=True).values_list("id", flat=True)
    else:
        return session_researcher.study_relations.filter(study__deleted=False).values_list("study__id", flat=True)


def validate_android_credentials(credentials):
    """Ensure basic formatting and field validation for android firebase credential json file uploads
    the credentials argument should contain a decoded string of such a file"""
    try:
        json_obj = json.dumps(credentials)
        # keys are inconsistent in presence, but these should be present in all.  (one is structure,
        # one is a critical data point.)
        if "project_info" not in json_obj or "project_id" not in json_obj:
            return False
    except Exception:
        return False
    return True


def validate_ios_credentials(credentials):
    """Ensure basic formatting and field validation for ios firebase credential plist file uploads
    the credentials argument should contain a decoded string of such a file"""
    try:
        plist_obj = plistlib.loads(str.encode(credentials))
        # ios has different key values than android, and they are somewhat opaque and inconsistently
        # present when generated. Just test for API_KEY
        if "API_KEY" not in plist_obj:
            return False
    except Exception:
        return False
    return True

####################################################################################################
######################################## Pages #####################################################
####################################################################################################


@system_admin_pages.route('/manage_researchers', methods=['GET'])
@authenticate_admin
def manage_researchers():
    # get the study names that each user has access to, but only those that the current admin  also
    # has access to.
    session_ids = get_session_researcher_study_ids()
    researcher_list = []
    for researcher in get_administerable_researchers():
        allowed_studies = Study.get_all_studies_by_name().filter(
            study_relations__researcher=researcher,
            study_relations__study__in=session_ids,
        ).values_list('name', flat=True)
        researcher_list.append((researcher.as_unpacked_native_python(), list(allowed_studies)))

    return render_template('manage_researchers.html', admins=researcher_list)


@system_admin_pages.route('/edit_researcher/<string:researcher_pk>', methods=['GET', 'POST'])
@authenticate_admin
def edit_researcher_page(researcher_pk):
    # Wow this got complex...
    session_researcher = get_session_researcher()
    edit_researcher = Researcher.objects.get(pk=researcher_pk)

    # site admins can edit study admins, but not other site admins.
    # (users do not edit their own passwords on this page.)
    editable_password = (
            not edit_researcher.username == session_researcher.username
            and not edit_researcher.site_admin
    )

    # if the session researcher is not a site admin then we need to restrict password editing
    # to only researchers that are not study_admins anywhere.
    if not session_researcher.site_admin:
        editable_password = editable_password and not edit_researcher.is_study_admin()

    # edit_study_info is a list of tuples of (study relationship, whether that study is editable by
    # the current session admin, and the study itself.)
    visible_studies = session_researcher.get_visible_studies_by_name()
    if edit_researcher.site_admin:
        # if the session admin is a site admin then we can skip the complex logic
        edit_study_info = [(SITE_ADMIN, True, study) for study in visible_studies]
    else:
        # When the session admin is just a study admin then we need to determine if the study that
        # the session admin can see is also one they are an admin on so we can display buttons.
        administerable_studies = set(get_administerable_studies_by_name().values_list("pk", flat=True))

        # We need the overlap of the edit_researcher studies with the studies visible to the session
        # admin, and we need those relationships for display purposes on the page.
        edit_study_relationship_map = {
            study_id: relationship.replace("_", " ").title()
            for study_id, relationship in edit_researcher.study_relations
                .filter(study__in=visible_studies)
                .values_list("study_id", "relationship")
        }

        # get the relevant studies, populate with relationship, editability, and the study.
        edit_study_info = []
        for study in visible_studies.filter(pk__in=edit_study_relationship_map.keys()):
            edit_study_info.append((
                edit_study_relationship_map[study.id],
                study.id in administerable_studies,
                study,
            ))

    return render_template(
        'edit_researcher.html',
        edit_researcher=edit_researcher,
        edit_study_info=edit_study_info,
        all_studies=get_administerable_studies_by_name(),  # this is all the studies administerable by the user
        editable_password=editable_password,
        redirect_url='/edit_researcher/{:s}'.format(researcher_pk),
        is_self=edit_researcher.id == session_researcher.id,
    )


@system_admin_pages.route('/elevate_researcher', methods=['POST'])
@authenticate_admin
def elevate_researcher_to_study_admin():
    researcher_pk = request.values.get("researcher_id")
    study_pk = request.values.get("study_id")
    assert_admin(study_pk)
    edit_researcher = Researcher.objects.get(pk=researcher_pk)
    study = Study.objects.get(pk=study_pk)
    assert_researcher_under_admin(edit_researcher, study)

    StudyRelation.objects.filter(researcher=edit_researcher, study=study)\
        .update(relationship=ResearcherRole.study_admin)

    return redirect(
        request.values.get("redirect_url", None) or '/edit_researcher/{:s}'.format(researcher_pk)
    )


@system_admin_pages.route('/demote_researcher', methods=['POST'])
@authenticate_admin
def demote_study_admin():
    researcher_pk = request.values.get("researcher_id")
    study_pk = request.values.get("study_id")
    assert_admin(study_pk)
    # assert_researcher_under_admin() would fail here...
    StudyRelation.objects.filter(
        researcher=Researcher.objects.get(pk=researcher_pk),
        study=Study.objects.get(pk=study_pk),
    ).update(relationship=ResearcherRole.researcher)
    return redirect(
        request.values.get("redirect_url", None) or '/edit_researcher/{:s}'.format(researcher_pk)
    )


@system_admin_pages.route('/create_new_researcher', methods=['GET', 'POST'])
@authenticate_admin
def create_new_researcher():
    if request.method == 'GET':
        return render_template('create_new_researcher.html')

    # Drop any whitespace or special characters from the username
    username = ''.join(e for e in request.form.get('admin_id', '') if e.isalnum())
    password = request.form.get('password', '')

    if Researcher.objects.filter(username=username).exists():
        flash(f"There is already a researcher with username {username}", 'danger')
        return redirect('/create_new_researcher')
    else:
        researcher = Researcher.create_with_password(username, password)
        return redirect('/edit_researcher/{:d}'.format(researcher.pk))


"""########################### Study Pages ##################################"""


@system_admin_pages.route('/manage_studies', methods=['GET'])
@authenticate_admin
def manage_studies():
    return render_template(
        'manage_studies.html',
        studies=[study.as_unpacked_native_python() for study in get_administerable_studies_by_name()]
    )


@system_admin_pages.route('/edit_study/<string:study_id>', methods=['GET'])
@authenticate_admin
def edit_study(study_id=None):
    # get the data points for display for all researchers in this study
    query = Researcher.filter_alphabetical(study_relations__study_id=study_id).values_list(
        "id", "username", "study_relations__relationship", "site_admin"
    )

    # transform raw query data as needed
    listed_researchers = []
    for pk, username, relationship, site_admin in query:
        listed_researchers.append((
            pk,
            username,
            "Site Admin" if site_admin else relationship.replace("_", " ").title(),
            site_admin
        ))

    return render_template(
        'edit_study.html',
        study=Study.objects.get(pk=study_id),
        administerable_researchers=get_administerable_researchers(),
        listed_researchers=listed_researchers,
        redirect_url='/edit_study/{:s}'.format(study_id),
        timezones=ALL_TIMEZONES_DROPDOWN,
    )


@system_admin_pages.route('/create_study', methods=['GET', 'POST'])
@authenticate_admin
def create_study():
    # Only a SITE admin can create new studies.
    if not get_session_researcher().site_admin:
        return abort(403)

    if request.method == 'GET':
        studies = [study.as_unpacked_native_python() for study in Study.get_all_studies_by_name()]
        return render_template('create_study.html', studies=studies)

    name = request.form.get('name', '')
    encryption_key = request.form.get('encryption_key', '')
    is_test = request.form.get('is_test', "").lower() == 'true'  # 'true' -> True, 'false' -> False
    duplicate_existing_study = request.form.get('copy_existing_study', None) == 'true'
    forest_enabled = request.form.get('forest_enabled', "").lower() == 'true'

    if len(name) > 5000:
        with make_error_sentry(SentryTypes.elastic_beanstalk):
            raise Exception("Someone tried to create a study with a suspiciously long name.")
        return abort(400)

    if escape(name) != name:
        with make_error_sentry(SentryTypes.elastic_beanstalk):
            raise Exception("Someone tried to create a study with unsafe characters in its name.")
        return abort(400)

    try:
        new_study = Study.create_with_object_id(
            name=name, encryption_key=encryption_key, is_test=is_test, forest_enabled=forest_enabled
        )
        if duplicate_existing_study:
            # surveys are always provided, there is a checkbox about whether to import them
            copy_device_settings = request.form.get('device_settings', None) == 'true'
            copy_surveys = request.form.get('surveys', None) == 'true'
            old_study = Study.objects.get(pk=request.form.get('existing_study_id', None))
            device_settings, surveys, interventions = unpack_json_study(format_study(old_study))

            copy_study_from_json(
                new_study,
                device_settings if copy_device_settings else {},
                surveys if copy_surveys else [],
                interventions,
            )
            Survey.TRACKING_SURVEY
            tracking_surveys_added = new_study.surveys.filter(survey_type=Survey.TRACKING_SURVEY).count()
            audio_surveys_added = new_study.surveys.filter(survey_type=Survey.AUDIO_SURVEY).count()
            # image_surveys_added = new_study.objects.filter(survey_type=Survey.IMAGE_SURVEY).count()
            flash(
                f"Copied {tracking_surveys_added} Surveys and {audio_surveys_added} " +
                f"Audio Surveys from {old_study.name} to {new_study.name}.",
                'success'
            )
            if copy_device_settings:
                flash(f"Overwrote {new_study.name}'s App Settings with custom values.", 'success')
            else:
                flash(f"Did not alter {new_study.name}'s App Settings.", 'success')

        flash(f'Successfully created study {name}.', 'success')
        return redirect('/device_settings/{:d}'.format(new_study.pk))

    except ValidationError as ve:
        # display message describing failure based on the validation error (hacky, but works.)
        for field, message in ve.message_dict.items():
            flash(f'{field}: {message[0]}', 'danger')
        return redirect('/create_study')


@system_admin_pages.route('/toggle_study_forest_enabled/<string:study_id>', methods=['POST'])
@authenticate_admin
def toggle_study_forest_enabled(study_id=None):
    # Only a SITE admin can toggle forest on a study
    if not get_session_researcher().site_admin:
        return abort(403)
    study = Study.objects.get(pk=study_id)
    study.forest_enabled = not study.forest_enabled
    study.save()
    if study.forest_enabled:
        flash("Enabled Forest on '%s'" % study.name, 'success')
    else:
        flash("Disabled Forest on '%s'" % study.name, 'success')
    return redirect('/edit_study/{:s}'.format(study_id))



# TODO: move to api file
@system_admin_pages.route('/delete_study/<string:study_id>', methods=['POST'])
@authenticate_admin
def delete_study(study_id=None):
    # Site admins and study admins can delete studies.
    assert_admin(study_id)

    if request.form.get('confirmation', 'false') == 'true':
        study = Study.objects.get(pk=study_id)
        study.deleted = True
        study.save()
        flash("Deleted study '%s'" % study.name, 'success')
        return "success"


@system_admin_pages.route('/device_settings/<string:study_id>', methods=['GET', 'POST'])
@authenticate_researcher_study_access
def device_settings(study_id=None):
    study = Study.objects.get(pk=study_id)
    researcher = get_session_researcher()
    readonly = True if not researcher.check_study_admin(study_id) and not researcher.site_admin else False

    # if read only....
    if request.method == 'GET':
        return render_template(
            "device_settings.html",
            study=study.as_unpacked_native_python(),
            settings=study.device_settings.as_unpacked_native_python(),
            readonly=readonly,
        )

    if readonly:
        abort(403)

    params = {k: v for k, v in request.values.items() if not k.startswith("consent_section")}
    consent_sections = {k: v for k, v in request.values.items() if k.startswith("consent_section")}
    params = checkbox_to_boolean(CHECKBOX_TOGGLES, params)
    params = string_to_int(TIMER_VALUES, params)
    # the ios consent sections are a json field but the frontend returns something weird,
    # see the documentation in unflatten_consent_sections for details
    params["consent_sections"] = json.dumps(unflatten_consent_sections(consent_sections))
    study.device_settings.update(**params)
    return redirect('/edit_study/{:d}'.format(study.id))


########################## FIREBASE CREDENTIALS ENDPOINTS ##################################
# note: all of the strings passed in the following function (eg: ALERT_DECODE_ERROR_TEXT) are plain strings
# not intended for use with .format or other potential injection vectors


@system_admin_pages.route('/manage_firebase_credentials')
@authenticate_admin
def manage_firebase_credentials():
    return render_template(
        'manage_firebase_credentials.html',
        firebase_credentials_exists=FileAsText.objects.filter(tag=BACKEND_FIREBASE_CREDENTIALS).exists(),
        android_credentials_exists=FileAsText.objects.filter(tag=ANDROID_FIREBASE_CREDENTIALS).exists(),
        ios_credentials_exists=FileAsText.objects.filter(tag=IOS_FIREBASE_CREDENTIALS).exists(),
    )


@system_admin_pages.route('/upload_backend_firebase_cert', methods=['POST'])
@authenticate_admin
def upload_firebase_cert():
    uploaded = request.files.get('backend_firebase_cert', None)

    if uploaded is None:
        flash(Markup(ALERT_EMPTY_TEXT), 'error')
        return redirect('/manage_firebase_credentials')

    try:
        cert = uploaded.read().decode()
    except UnicodeDecodeError:  # raised for an unexpected file type
        flash(Markup(ALERT_DECODE_ERROR_TEXT), 'error')
        return redirect('/manage_firebase_credentials')

    if not cert:
        flash(Markup(ALERT_EMPTY_TEXT), 'error')
        return redirect('/manage_firebase_credentials')

    instantiation_errors = get_firebase_credential_errors(cert)
    if instantiation_errors:
        # noinspection StrFormat
        # This string is sourced purely from the error message of get_firebase_credential_errors,
        # all of which are known-safe text. (no javascript injection)
        error_string = ALERT_SPECIFIC_ERROR_TEXT.format(error_message=instantiation_errors)
        flash(Markup(error_string), 'error')
        return redirect('/manage_firebase_credentials')

    # delete and recreate to get metadata timestamps
    FileAsText.objects.filter(tag=BACKEND_FIREBASE_CREDENTIALS).delete()
    FileAsText.objects.create(tag=BACKEND_FIREBASE_CREDENTIALS, text=cert)
    update_firebase_instance()
    flash(Markup(ALERT_SUCCESS_TEXT), 'info')
    return redirect('/manage_firebase_credentials')


@system_admin_pages.route('/upload_android_firebase_cert', methods=['POST'])
@authenticate_admin
def upload_android_firebase_cert():
    uploaded = request.files.get('android_firebase_cert', None)
    try:
        if uploaded is None:
            raise AssertionError("file name missing from upload")
        cert = uploaded.read().decode()
        if not cert:
            raise AssertionError("unexpected empty string")
        if not validate_android_credentials(cert):
            raise ValidationError('wrong keys for android cert')
        FileAsText.objects.get_or_create(tag=ANDROID_FIREBASE_CREDENTIALS, defaults={"text": cert})
        flash(Markup(ALERT_ANDROID_SUCCESS_TEXT), 'info')
    except AssertionError:
        flash(Markup(ALERT_EMPTY_TEXT), 'error')
    except UnicodeDecodeError:  # raised for an unexpected file type
        flash(Markup(ALERT_DECODE_ERROR_TEXT), 'error')
    except ValidationError:
        flash(Markup(ALERT_ANDROID_VALIDATION_FAILED_TEXT), 'error')
    except AttributeError:  # raised for a missing file
        flash(Markup(ALERT_EMPTY_TEXT), 'error')
    except ValueError:
        flash(Markup(ALERT_MISC_ERROR_TEXT), 'error')
    return redirect('/manage_firebase_credentials')


@system_admin_pages.route('/upload_ios_firebase_cert', methods=['POST'])
@authenticate_admin
def upload_ios_firebase_cert():
    uploaded = request.files.get('ios_firebase_cert', None)
    try:
        if uploaded is None:
            raise AssertionError("file name missing from upload")
        cert = uploaded.read().decode()
        if not cert:
            raise AssertionError("unexpected empty string")
        if not validate_ios_credentials(cert):
            raise ValidationError('wrong keys for ios cert')
        FileAsText.objects.get_or_create(tag=IOS_FIREBASE_CREDENTIALS, defaults={"text": cert})
        flash(Markup(ALERT_IOS_SUCCESS_TEXT), 'info')
    except AssertionError:
        flash(Markup(ALERT_EMPTY_TEXT), 'error')
    except UnicodeDecodeError:  # raised for an unexpected file type
        flash(Markup(ALERT_DECODE_ERROR_TEXT), 'error')
    except AttributeError:  # raised for a missing file
        flash(Markup(ALERT_EMPTY_TEXT), 'error')
    except ValidationError:
        flash(Markup(ALERT_IOS_VALIDATION_FAILED_TEXT), 'error')
    except ValueError:
        flash(Markup(ALERT_MISC_ERROR_TEXT), 'error')
    return redirect('/manage_firebase_credentials')


@system_admin_pages.route('/delete_backend_firebase_cert', methods=['POST'])
@authenticate_admin
def delete_backend_firebase_cert():
    FileAsText.objects.filter(tag=BACKEND_FIREBASE_CREDENTIALS).delete()
    # deletes the existing firebase app connection to clear credentials from memory
    update_firebase_instance()
    flash(Markup(ALERT_FIREBASE_DELETED_TEXT), 'info')
    return redirect('/manage_firebase_credentials')


@system_admin_pages.route('/delete_android_firebase_cert', methods=['POST'])
@authenticate_admin
def delete_android_firebase_cert():
    FileAsText.objects.filter(tag=ANDROID_FIREBASE_CREDENTIALS).delete()
    flash(Markup(ALERT_ANDROID_DELETED_TEXT), 'info')
    return redirect('/manage_firebase_credentials')


@system_admin_pages.route('/delete_ios_firebase_cert', methods=['POST'])
@authenticate_admin
def delete_ios_firebase_cert():
    FileAsText.objects.filter(tag=IOS_FIREBASE_CREDENTIALS).delete()
    flash(Markup(ALERT_IOS_DELETED_TEXT), 'info')
    return redirect('/manage_firebase_credentials')
