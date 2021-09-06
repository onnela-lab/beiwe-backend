from datetime import date, datetime
from django.core.paginator import EmptyPage
from flask import abort, Blueprint, flash, redirect, render_template, request

from api.participant_administration import add_fields_and_interventions
from authentication.admin_authentication import (authenticate_researcher_study_access,
    get_researcher_allowed_studies, researcher_is_an_admin)
from config.constants import API_DATE_FORMAT
from database.schedule_models import ArchivedEvent
from database.study_models import Study
from database.user_models import Participant
from libs.push_notification_config import (check_firebase_instance,
    repopulate_all_survey_scheduled_events)

participant_pages = Blueprint('participant_pages', __name__)


@participant_pages.context_processor
def inject_html_params():
    # these variables will be accessible to every template rendering attached to the blueprint
    return {
        "allowed_studies": get_researcher_allowed_studies(),
        "is_admin": researcher_is_an_admin(),
    }


@participant_pages.route('/view_study/<string:study_id>/participant/<string:patient_id>/notification_history', methods=['GET'])
@authenticate_researcher_study_access
def notification_history(study_id, patient_id):
    try:
        participant = Participant.objects.get(patient_id=patient_id)
        study = participant.study
    except Participant.DoesNotExist:
        return abort(404)
    page_number = request.args.get('page', 1)
    per_page = request.args.get('per_page', 100)
    survey_names = get_survey_names_dict(study)
    notification_attempts = []
    archived_events = ArchivedEvent.get_values_for_notification_history_paginated(participant.id, per_page=per_page)
    try:
        archived_events_page = archived_events.page(page_number)
    except EmptyPage:
        return abort(404)
    last_page_number = archived_events.page_range.stop - 1
    for archived_event in archived_events_page:
        notification_attempts.append(get_notification_details(archived_event, study.timezone, survey_names))
    return render_template('notification_history.html', participant=participant, page=archived_events_page,
                           notification_attempts=notification_attempts, study=study, last_page_number=last_page_number)


@participant_pages.route('/view_study/<string:study_id>/participant/<string:patient_id>', methods=['GET', 'POST'])
@authenticate_researcher_study_access
def participant(study_id, patient_id):
    try:
        participant = Participant.objects.get(patient_id=patient_id)
        study = participant.study
    except Participant.DoesNotExist:
        return abort(404)

    # safety check, enforce fields and interventions to be present for both page load and edit.
    add_fields_and_interventions(participant, study)

    if request.method == 'GET':
        return render_participant_page(participant, study)

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

    # always call through the repopulate everything call, even though we only need to handle
    # relative surveys, the function handles extra cases.
    repopulate_all_survey_scheduled_events(study, participant)

    flash('Successfully edited participant {}.'.format(participant.patient_id), 'success')
    return redirect(request.referrer)


def render_participant_page(participant: Participant, study: Study):
    # to reduce database queries we get all the data across 4 queries and then merge it together.
    # dicts of intervention id to intervention date string, and of field names to value
    # (this was quite slow previously)
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

    notification_attempts_count = participant.archived_events.count()
    survey_names = get_survey_names_dict(study)
    last_archived_event = ArchivedEvent.get_values_for_most_recent_notification(participant.id)
    latest_notification_attempt = \
        get_notification_details(last_archived_event, study.timezone, survey_names)

    return render_template(
        'participant.html',
        participant=participant,
        study=study,
        intervention_data=intervention_data,
        field_values=field_data,
        notification_attempts_count=notification_attempts_count,
        latest_notification_attempt=latest_notification_attempt,
        push_notifications_enabled_for_ios=check_firebase_instance(require_ios=True),
        push_notifications_enabled_for_android=check_firebase_instance(require_android=True)
    )


def get_survey_names_dict(study):
    survey_names = {}
    for survey in study.surveys.all():
        survey_name = ("Audio Survey " if survey.survey_type == 'audio_survey' else "Survey ") + survey.object_id
        survey_names[survey.id] = survey_name
    return survey_names


def get_notification_details(archived_event, study_timezone, survey_names):
    # Maybe there's a less janky way to get timezone name, but I don't know what it is:
    timezone_short_name = study_timezone.tzname(datetime.now().astimezone(study_timezone))

    def format_datetime(dt):
        return dt.astimezone(study_timezone).strftime('%A %b %-d, %Y, %-I:%M %p') + " (" + timezone_short_name + ")"

    notification = {}
    if archived_event is not None:
        notification['scheduled_time'] = format_datetime(archived_event['scheduled_time'])
        notification['attempted_time'] = format_datetime(archived_event['created_on'])
        notification['survey_name'] = survey_names[archived_event['survey_id']]
        notification['survey_id'] = archived_event['survey_id']
        notification['survey_version'] = archived_event['survey_version'].strftime('%Y-%m-%d')
        notification['schedule_type'] = archived_event['schedule_type']
        notification['status'] = archived_event['status']

    return notification
