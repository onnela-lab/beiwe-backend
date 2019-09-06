import json
from collections import defaultdict

from django.core.exceptions import ValidationError
from django.db.models.deletion import ProtectedError
from flask import (abort, Blueprint, flash, redirect, render_template, request)

from config.constants import CHECKBOX_TOGGLES, TIMER_VALUES
from database.study_models import Study, StudyField
from database.user_models import Researcher
from libs.admin_authentication import (authenticate_researcher_study_access,
    authenticate_site_admin, get_researcher_allowed_studies, get_session_researcher,
    researcher_is_an_admin)
from libs.copy_study import copy_existing_study_if_asked_to
from libs.http_utils import checkbox_to_boolean, string_to_int

system_admin_pages = Blueprint('system_admin_pages', __name__)

####################################################################################################
###################################### Helpers #####################################################
####################################################################################################

def get_administerable_studies():
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
        relevant_researchers = Researcher.get_all_researchers_by_username()
    else:
        relevant_researchers = researcher_admin.get_administered_researchers_by_username()
    return relevant_researchers


def unflatten_consent_sections(consent_sections_dict):
    # consent_sections is a flat structure with structure like this:
    # { 'label_ending_in.text': 'text content',  'label_ending_in.more': 'more content' }
    # we need to transform it into a nested structure like this:
    # { 'label': {'text':'text content',  'more':'more content' }
    refactored_consent_sections = defaultdict(dict)
    for key, content in consent_sections_dict.iteritems():
        _, label, content_type = key.split(".")
        # print _, label, content_type
        refactored_consent_sections[label][content_type] = content
    return dict(refactored_consent_sections)


####################################################################################################
######################################## Pages #####################################################
####################################################################################################

@system_admin_pages.route('/manage_researchers', methods=['GET'])
@authenticate_site_admin
def manage_researchers():
    session_researcher = get_session_researcher()
    if session_researcher.site_admin:
        study_ids = Study.objects.exclude(deleted=True).values_list("id", flat=True)
    else:
        study_ids = session_researcher.study_relations.values_list("study_id", flat=True)

    researcher_list = []
    # get the study names that each user has access to, but only those that the current admin  also
    # has access to.
    for researcher in get_administerable_researchers():
        allowed_studies = Study.get_all_studies_by_name().filter(
            study_relations__researcher=researcher, study_relations__study__in=study_ids,
        ).values_list('name', flat=True)
        researcher_list.append((researcher.as_native_python(), list(allowed_studies)))

    return render_template(
        'manage_researchers.html',
        admins=json.dumps(researcher_list),
        allowed_studies=get_researcher_allowed_studies(),
        is_admin=researcher_is_an_admin()
    )


@system_admin_pages.route('/edit_researcher/<string:researcher_pk>', methods=['GET', 'POST'])
@authenticate_site_admin
def edit_researcher(researcher_pk):
    edit_researcher = Researcher.objects.get(pk=researcher_pk)
    is_session_researcher = edit_researcher.username == get_session_researcher().username,
    return render_template(
        'edit_researcher.html',
        admin=edit_researcher,
        current_studies=Study.get_all_studies_by_name().filter(study_relations__researcher=edit_researcher),
        all_studies=get_administerable_studies(),
        allowed_studies=get_researcher_allowed_studies(),
        is_session_researcher=is_session_researcher,
        is_admin=researcher_is_an_admin(),
        redirect_url='/edit_researcher/{:s}'.format(researcher_pk),
    )


@system_admin_pages.route('/create_new_researcher', methods=['GET', 'POST'])
@authenticate_site_admin
def create_new_researcher():
    if request.method == 'GET':
        return render_template(
            'create_new_researcher.html',
            allowed_studies=get_researcher_allowed_studies(),
            is_admin=researcher_is_an_admin()
        )

    # Drop any whitespace or special characters from the username
    username = ''.join(e for e in request.form.get('admin_id', '') if e.isalnum())
    password = request.form.get('password', '')

    if Researcher.objects.filter(username=username).exists():
        flash("There is already a researcher with username " + username, 'danger')
        return redirect('/create_new_researcher')
    else:
        researcher = Researcher.create_with_password(username, password)
        return redirect('/edit_researcher/{:d}'.format(researcher.pk))


"""########################### Study Pages ##################################"""


@system_admin_pages.route('/manage_studies', methods=['GET'])
@authenticate_site_admin
def manage_studies():
    return render_template(
        'manage_studies.html',
        studies=json.dumps([study.as_native_python() for study in get_administerable_studies()]),
        allowed_studies=get_researcher_allowed_studies(),
        is_admin=researcher_is_an_admin()
    )


@system_admin_pages.route('/edit_study/<string:study_id>', methods=['GET'])
@authenticate_site_admin
def edit_study(study_id=None):
    return render_template(
        'edit_study.html',
        study=Study.objects.get(pk=study_id),
        all_researchers=get_administerable_researchers(),
        allowed_studies=get_researcher_allowed_studies(),
        is_admin=researcher_is_an_admin(),
        redirect_url='/edit_study/{:s}'.format(study_id),
    )


@system_admin_pages.route('/study_fields/<string:study_id>', methods=['GET', 'POST'])
@authenticate_researcher_study_access
def study_fields(study_id=None):
    study = Study.objects.get(pk=study_id)
    researcher = get_session_researcher()
    readonly = True if not researcher.check_study_admin(study_id) and not researcher.site_admin else False

    if request.method == 'GET':
        return render_template(
            'study_custom_fields.html',
            study=study,
            fields=study.fields.all(),
            readonly=readonly,
            allowed_studies=get_researcher_allowed_studies(),
            is_admin=researcher_is_an_admin(),
        )

    if readonly:
        abort(403)

    new_field = request.values.get('new_field', None)
    if new_field:
        StudyField.objects.get_or_create(study=study, field_name=new_field)

    return redirect('/study_fields/{:d}'.format(study.id))


@system_admin_pages.route('/delete_field/<string:study_id>', methods=['POST'])
@authenticate_researcher_study_access
def delete_field(study_id=None):
    study = Study.objects.get(pk=study_id)
    researcher = get_session_researcher()
    readonly = True if not researcher.check_study_admin(study_id) and not researcher.site_admin else False
    if readonly:
        abort(403)

    field = request.values.get('field', None)
    if field:
        try:
            study_field = StudyField.objects.get(study=study, id=field)
        except StudyField.DoesNotExist:
            study_field = None

        try:
            if study_field:
                study_field.delete()
        except ProtectedError:
            flash("This field can not be removed because it is already in use", 'danger')

    return redirect('/study_fields/{:d}'.format(study.id))


@system_admin_pages.route('/create_study', methods=['GET', 'POST'])
@authenticate_site_admin
def create_study():
    # ONLY THE SITE ADMIN CAN CREATE NEW STUDIES.
    if not get_session_researcher().site_admin:
        return abort(403)

    if request.method == 'GET':
        studies = [study.as_native_python() for study in Study.get_all_studies_by_name()]
        return render_template(
            'create_study.html',
            studies=json.dumps(studies),
            allowed_studies=get_researcher_allowed_studies(),
            is_admin=researcher_is_an_admin()
        )

    name = request.form.get('name', '')
    encryption_key = request.form.get('encryption_key', '')
    is_test = request.form.get('is_test') == 'true'  # 'true' -> True, 'false' -> False

    assert len(name) <= 2 ** 16, "safety check on new study name failed"

    try:
        study = Study.create_with_object_id(name=name, encryption_key=encryption_key, is_test=is_test)
        copy_existing_study_if_asked_to(study)
        flash('Successfully created study {}.'.format(name), 'success')
        return redirect('/device_settings/{:d}'.format(study.pk))
    except ValidationError as ve:
        for field, message in ve.message_dict.iteritems():
            flash('{}: {}'.format(field, message[0]), 'danger')
        return redirect('/create_study')


@system_admin_pages.route('/delete_study/<string:study_id>', methods=['POST'])
@authenticate_site_admin
def delete_study(study_id=None):
    """ This functionality has been disabled pending testing and feature change."""
    # ONLY THE SITE ADMIN CAN DELETE A STUDY.
    if not get_session_researcher().site_admin:
        return abort(403)

    if request.form.get('confirmation', 'false') == 'true':
        study = Study.objects.get(pk=study_id)
        study.mark_deleted()
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
            study=study.as_native_python(),
            settings=study.get_study_device_settings().as_native_python(),
            readonly=readonly,
            allowed_studies=get_researcher_allowed_studies(),
            is_admin=researcher_is_an_admin()
        )

    if readonly:
        abort(403)
        
    settings = study.get_study_device_settings()
    params = {k:v for k,v in request.values.iteritems() if not k.startswith("consent_section")}
    consent_sections = {k: v for k, v in request.values.iteritems() if k.startswith("consent_section")}
    params = checkbox_to_boolean(CHECKBOX_TOGGLES, params)
    params = string_to_int(TIMER_VALUES, params)
    # the ios consent sections are a json field but the frontend returns something weird,
    # see the documentation in unflatten_consent_sections for details
    params["consent_sections"] = json.dumps(unflatten_consent_sections(consent_sections))
    settings.update(**params)
    return redirect('/edit_study/{:d}'.format(study.id))


