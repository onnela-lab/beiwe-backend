from datetime import date, datetime

from django.db.models import ProtectedError
from flask import abort, Blueprint, flash, redirect, render_template, request

from config.constants import API_DATE_FORMAT
from database.schedule_models import Intervention, InterventionDate
from database.study_models import Study, StudyField
from database.user_models import Participant, ParticipantFieldValue
from authentication.admin_authentication import (authenticate_researcher_study_access,
    get_researcher_allowed_studies, get_session_researcher, researcher_is_an_admin)
from libs.push_notifications import check_firebase_instance

study_api = Blueprint('study_api', __name__)


@study_api.context_processor
def inject_html_params():
    # these variables will be accessible to every template rendering attached to the blueprint
    return {
        "allowed_studies": get_researcher_allowed_studies(),
        "is_admin": researcher_is_an_admin(),
    }


@study_api.route('/view_study/<string:study_id>/edit_participant/<string:participant_id>', methods=['GET', 'POST'])
@authenticate_researcher_study_access
def edit_participant(study_id, participant_id):
    try:
        participant = Participant.objects.get(pk=participant_id)
    except Participant.DoesNotExist:
        return abort(404)

    study = participant.study

    if request.method == 'GET':
        return render_edit_participant(participant, study)

    # update intervention dates for participant
    for intervention in study.interventions.all():
        input_date = request.values.get(f"intervention{intervention.id}", None)
        intervention_date = participant.intervention_dates.get(intervention=intervention)
        if input_date:
            intervention_date.date = datetime.strptime(input_date, API_DATE_FORMAT).date()
            intervention_date.save()

    # update custom fields dates for participant
    for field in study.fields.all():
        input_id = f"field{field.id}"
        field_value = participant.field_values.get(field=field)
        field_value.value = request.values.get(input_id, None)
        field_value.save()

    flash('Successfully edited participant {}.'.format(participant.patient_id), 'success')
    return redirect('/view_study/{:d}/edit_participant/{:d}'.format(study.id, participant.id))


def render_edit_participant(participant: Participant, study: Study):
    # to reduce database queries we get all the data across 4 queries and then merge it together.
    # dicts of intervention id to intervention date string, and of field names to value
    intervention_dates_map = {
        intervention_id:  # this is the intervention's id, not the intervention_date's id.
            intervention_date.strftime(API_DATE_FORMAT) if isinstance(intervention_date, date) else ""
        for intervention_id, intervention_date in
        participant.intervention_dates.values_list("intervention_id", "date")
    }
    participant_fields_map = {
        name: value for name, value in participant.field_values.values_list("field__field_name", "value")
    }

    # list of tuples of (intervention id, intervention name, intervention date)
    intervention_data = [
        (intervention.id, intervention.name, intervention_dates_map.get(intervention.id, ""))
        for intervention in study.interventions.order_by("name")
    ]
    # list of tuples of field name, value.
    field_data = [
        (field_id, field_name, participant_fields_map.get(field_name, ""))
        for field_id, field_name in study.fields.order_by("field_name").values_list('id', "field_name")
    ]

    return render_template(
        'edit_participant.html',
        participant=participant,
        study=study,
        intervention_data=intervention_data,
        field_values=field_data,
        push_notifications_enabled_for_ios=check_firebase_instance(require_ios=True),
        push_notifications_enabled_for_android=check_firebase_instance(require_android=True)
    )

@study_api.route('/interventions/<string:study_id>', methods=['GET', 'POST'])
@authenticate_researcher_study_access
def interventions(study_id=None):
    study = Study.objects.get(pk=study_id)
    researcher = get_session_researcher()
    readonly = True if not researcher.check_study_admin(study_id) and not researcher.site_admin else False

    if request.method == 'GET':
        return render_template(
            'study_interventions.html',
            study=study,
            interventions=study.interventions.all(),
            readonly=readonly,
        )

    if readonly:
        abort(403)

    new_intervention = request.values.get('new_intervention', None)
    if new_intervention:
        intervention, _ = Intervention.objects.get_or_create(study=study, name=new_intervention)
        for participant in study.participants.all():
            InterventionDate.objects.get_or_create(participant=participant, intervention=intervention)

    return redirect('/interventions/{:d}'.format(study.id))


@study_api.route('/delete_intervention/<string:study_id>', methods=['POST'])
@authenticate_researcher_study_access
def delete_intervention(study_id=None):
    """Deletes the specified Intervention. Expects intervention in the request body."""
    study = Study.objects.get(pk=study_id)
    researcher = get_session_researcher()
    readonly = True if not researcher.check_study_admin(study_id) and not researcher.site_admin else False
    if readonly:
        return abort(403)

    intervention_id = request.values.get('intervention')
    if intervention_id:
        try:
            intervention = Intervention.objects.get(id=intervention_id)
        except Intervention.DoesNotExist:
            intervention = None
        try:
            if intervention:
                intervention.delete()
        except ProtectedError:
            flash("This Intervention can not be removed because it is already in use", 'danger')

    return redirect('/interventions/{:d}'.format(study.id))


@study_api.route('/edit_intervention/<string:study_id>', methods=['POST'])
@authenticate_researcher_study_access
def edit_intervention(study_id=None):
    """
    Edits the name of the intervention. Expects intervention_id and edit_intervention in the
    request body
    """
    study = Study.objects.get(pk=study_id)
    researcher = get_session_researcher()
    readonly = True if not researcher.check_study_admin(study_id) and not researcher.site_admin else False
    if readonly:
        return abort(403)

    intervention_id = request.values.get('intervention_id', None)
    new_name = request.values.get('edit_intervention', None)
    if intervention_id:
        try:
            intervention = Intervention.objects.get(id=intervention_id)
        except Intervention.DoesNotExist:
            intervention = None
        if intervention and new_name:
            intervention.name = new_name
            intervention.save()

    return redirect('/interventions/{:d}'.format(study.id))


@study_api.route('/study_fields/<string:study_id>', methods=['GET', 'POST'])
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
        )

    if readonly:
        return abort(403)

    new_field = request.values.get('new_field', None)
    if new_field:
        study_field, _ = StudyField.objects.get_or_create(study=study, field_name=new_field)
        for participant in study.participants.all():
            ParticipantFieldValue.objects.create(participant=participant, field=study_field)

    return redirect('/study_fields/{:d}'.format(study.id))


@study_api.route('/delete_field/<string:study_id>', methods=['POST'])
@authenticate_researcher_study_access
def delete_field(study_id=None):
    """Deletes the specified Custom Field. Expects field in the request body."""
    study = Study.objects.get(pk=study_id)
    researcher = get_session_researcher()
    readonly = True if not researcher.check_study_admin(study_id) and not researcher.site_admin else False
    if readonly:
        return abort(403)

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


@study_api.route('/edit_custom_field/<string:study_id>', methods=['POST'])
@authenticate_researcher_study_access
def edit_custom_field(study_id=None):
    """Edits the name of a Custom field. Expects field_id anf edit_custom_field in request body"""

    study = Study.objects.get(pk=study_id)
    researcher = get_session_researcher()
    readonly = True if not researcher.check_study_admin(
        study_id) and not researcher.site_admin else False
    if readonly:
        return abort(403)

    field_id = request.values.get("field_id")
    new_field_name = request.values.get("edit_custom_field")
    if field_id:
        try:
            field = StudyField.objects.get(id=field_id)
        except StudyField.DoesNotExist:
            field = None
        if field and new_field_name:
            field.field_name = new_field_name
            field.save()

    return redirect('/study_fields/{:d}'.format(study.id))
