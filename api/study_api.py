from django.db.models import ProtectedError
from flask import Blueprint, flash, redirect, render_template, request

from authentication.admin_authentication import (authenticate_researcher_study_access,
    get_researcher_allowed_studies, researcher_is_an_admin)
from database.schedule_models import Intervention, InterventionDate
from database.study_models import Study, StudyField
from database.user_models import Participant, ParticipantFieldValue

study_api = Blueprint('study_api', __name__)


@study_api.context_processor
def inject_html_params():
    # these variables will be accessible to every template rendering attached to the blueprint
    return {
        "allowed_studies": get_researcher_allowed_studies(),
        "is_admin": researcher_is_an_admin(),
    }


@study_api.route('/study/<string:study_id>/get_participants_api', methods=['GET'])
def get_participants_api(study_id):
    study = Study.objects.get(pk=study_id)
    # `draw` is passed by DataTables. It's automatically incremented, starting with 1 on the page
    # load, and then 2 with the next call to this API endpoint, and so on.
    draw = int(request.args.get('draw'))
    start = int(request.args.get('start'))
    length = int(request.args.get('length'))
    sort_by_column_index = int(request.args.get('order[0][column]'))
    sort_in_descending_order = request.args.get('order[0][dir]') == 'desc'
    contains_string = request.args.get('search[value]')
    total_participants_count = Participant.objects.filter(study_id=study_id).count()
    filtered_participants_count = (study.filtered_participants(contains_string).count())
    data = study.get_values_for_participants_table(start, length, sort_by_column_index,
                                                   sort_in_descending_order, contains_string)
    table_data = {
        "draw": draw,
        "recordsTotal": total_participants_count,
        "recordsFiltered": filtered_participants_count,
        "data": data
    }
    return table_data


@study_api.route('/interventions/<string:study_id>', methods=['GET', 'POST'])
@authenticate_researcher_study_access
def interventions_page(study_id=None):
    study = Study.objects.get(pk=study_id)

    if request.method == 'GET':
        return render_template(
            'study_interventions.html',
            study=study,
            interventions=study.interventions.all(),
        )

    # slow but safe
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

    if request.method == 'GET':
        return render_template(
            'study_custom_fields.html',
            study=study,
            fields=study.fields.all(),
        )

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

    # this apparent insanity is a hopefully unnecessary confirmation of the study id
    return redirect('/study_fields/{:d}'.format(Study.objects.get(pk=study_id).id))
