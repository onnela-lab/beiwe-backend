from __future__ import print_function

import json
from collections import defaultdict

from django.core.exceptions import ValidationError
from django.db.models.deletion import ProtectedError
from flask import (abort, Blueprint, flash, redirect, render_template, request)

from config.constants import CHECKBOX_TOGGLES, ResearcherRole, TIMER_VALUES
from database.study_models import Study, StudyField
from database.user_models import Researcher, StudyRelation
from libs.admin_authentication import (assert_admin, assert_researcher_under_admin,
    authenticate_admin, authenticate_researcher_study_access, get_researcher_allowed_studies,
    get_session_researcher, researcher_is_an_admin)
from libs.copy_study import copy_existing_study_if_asked_to
from libs.http_utils import checkbox_to_boolean, string_to_int

system_admin_pages = Blueprint('system_admin_pages', __name__)

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


def get_session_researcher_study_ids():
    """ Returns the appropriate study ids based on whether a user is a study or site admin """
    session_researcher = get_session_researcher()
    if session_researcher.site_admin:
        return Study.objects.exclude(deleted=True).values_list("id", flat=True)
    else:
        return session_researcher.study_relations.filter(study__deleted=False).values_list("study__id", flat=True)


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
        researcher_list.append((researcher.as_native_python(), list(allowed_studies)))

    return render_template(
        'manage_researchers.html',
        admins=json.dumps(researcher_list),
        allowed_studies=get_researcher_allowed_studies(),
        is_admin=researcher_is_an_admin()
    )


@system_admin_pages.route('/edit_researcher/<string:researcher_pk>', methods=['GET', 'POST'])
@authenticate_admin
def edit_researcher(researcher_pk):
    session_researcher = get_session_researcher()
    edit_researcher = Researcher.objects.get(pk=researcher_pk)

    # site admins can edit study admins, but not other site admins.
    # (users do not edit their own passwords on this page.)
    editable_password = (
            not edit_researcher.username == get_session_researcher().username
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
        edit_study_info = [("Site Admin", True, study) for study in visible_studies]
    else:
        # When the session admin is just a study admin then we need to determine if the study that
        # the session admin can see is also one they are an admin on so we can display buttons.
        administerable_studies = set(get_administerable_studies_by_name().values_list("pk", flat=True))

        # We need the overlap of the edit_researcher studies with the studies visible to the session
        # admin, and we need those relationships for display purposes on the page.
        edit_study_relationship_map = {
            study_id: relationship.replace("_", " ").title()
            for study_id, relationship in edit_researcher.study_relations.filter(study__in=visible_studies).values_list("study_id", "relationship")
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
        allowed_studies=get_researcher_allowed_studies(),
        editable_password=editable_password,
        is_admin=researcher_is_an_admin(),
        redirect_url='/edit_researcher/{:s}'.format(researcher_pk),
        session_researcher=session_researcher,
        is_self=edit_researcher.id==session_researcher.id,
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
    StudyRelation.objects.filter(
        researcher=edit_researcher,
        study=study,
    ).update(relationship=ResearcherRole.study_admin)
    redirect_url = request.values.get("redirect_url", None) or '/edit_researcher/{:s}'.format(researcher_pk)
    return redirect(redirect_url)


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

    redirect_url = request.values.get("redirect_url", None) or '/edit_researcher/{:s}'.format(researcher_pk)
    return redirect(redirect_url)


@system_admin_pages.route('/create_new_researcher', methods=['GET', 'POST'])
@authenticate_admin
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
@authenticate_admin
def manage_studies():
    return render_template(
        'manage_studies.html',
        studies=json.dumps([study.as_native_python() for study in get_administerable_studies_by_name()]),
        allowed_studies=get_researcher_allowed_studies(),
        is_admin=researcher_is_an_admin(),
        session_researcher=get_session_researcher(),
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
        allowed_studies=get_researcher_allowed_studies(),
        listed_researchers=listed_researchers,
        is_admin=researcher_is_an_admin(),
        redirect_url='/edit_study/{:s}'.format(study_id),
        session_researcher=get_session_researcher(),
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
@authenticate_admin
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
@authenticate_admin
def delete_study(study_id=None):
    # Site admins and study admins can delete studies.
    assert_admin(study_id)

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


