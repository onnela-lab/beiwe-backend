from datetime import date, datetime
from typing import Dict

from django.contrib import messages
from django.core.paginator import EmptyPage, Paginator
from django.db import transaction
from django.db.models import F
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_GET, require_http_methods

from api.participant_administration import add_fields_and_interventions
from authentication.admin_authentication import authenticate_researcher_study_access
from constants.common_constants import API_DATE_FORMAT
from database.schedule_models import ArchivedEvent, ParticipantMessage, ParticipantMessageStatus
from database.study_models import Study
from database.user_models import Participant
from libs.firebase_config import check_firebase_instance
from libs.forms import ParticipantMessageForm
from libs.http_utils import easy_url
from libs.internal_types import ArchivedEventQuerySet, ResearcherRequest
from libs.push_notification_helpers import repopulate_all_survey_scheduled_events
from middleware.abort_middleware import abort


@require_GET
@authenticate_researcher_study_access
def notification_history(request: ResearcherRequest, study_id: int, patient_id: str):
    # use the provided study id because authentication already validated it
    participant = get_object_or_404(Participant, patient_id=patient_id)
    study = get_object_or_404(Study, pk=study_id)
    page_number = request.GET.get('page', 1)
    per_page = request.GET.get('per_page', 100)
    
    archived_events = Paginator(query_values_for_notification_history(participant.id), per_page)
    try:
        archived_events_page = archived_events.page(page_number)
    except EmptyPage:
        return abort(404)
    last_page_number = archived_events.page_range.stop - 1
    
    survey_names = get_survey_names_dict(study)
    notification_attempts = [
        get_notification_details(archived_event, study.timezone, survey_names)
        for archived_event in archived_events_page
    ]
    return render(
        request,
        'notification_history.html',
        context=dict(
            participant=participant,
            page=archived_events_page,
            notification_attempts=notification_attempts,
            study=study,
            last_page_number=last_page_number,
        )
    )


@require_http_methods(['GET', 'POST'])
@authenticate_researcher_study_access
def participant_page(request: ResearcherRequest, study_id: int, patient_id: str):
    # use the provided study id because authentication already validated it
    try:
        participant = Participant.objects.get(patient_id=patient_id)
        study = Study.objects.get(id=study_id)
    except (Participant.DoesNotExist, Study.DoesNotExist):
        return abort(404)
    
    # safety check, enforce fields and interventions to be present for both page load and edit.
    add_fields_and_interventions(participant, study)
    
    # FIXME: get rid of dual endpoint pattern, it is a bad idea.
    if request.method == 'GET':
        return render_participant_page(request, participant, study)
    
    # update intervention dates for participant
    for intervention in study.interventions.all():
        input_date = request.POST.get(f"intervention{intervention.id}", None)
        intervention_date = participant.intervention_dates.get(intervention=intervention)
        if input_date:
            intervention_date.update(date=datetime.strptime(input_date, API_DATE_FORMAT).date())
    
    # update custom fields dates for participant
    for field in study.fields.all():
        input_id = f"field{field.id}"
        field_value = participant.field_values.get(field=field)
        field_value.update(value=request.POST.get(input_id, None))
    
    # always call through the repopulate everything call, even though we only need to handle
    # relative surveys, the function handles extra cases.
    repopulate_all_survey_scheduled_events(study, participant)
    
    messages.success(request, f'Successfully edited participant {participant.patient_id}.')
    return redirect(easy_url(
        "participant_pages.participant_page", study_id=study_id, patient_id=patient_id
    ))

def render_participant_page(request: ResearcherRequest, participant: Participant, study: Study):
    # to reduce database queries we get all the data across 4 queries and then merge it together.
    # dicts of intervention id to intervention date string, and of field names to value
    # (this was quite slow previously)
    intervention_dates_map = {
        # this is the intervention's id, not the intervention_date's id.
        intervention_id: format_date_or_none(intervention_date)
        for intervention_id, intervention_date in
        participant.intervention_dates.values_list("intervention_id", "date")
    }
    participant_fields_map = {
        name: value for name, value in
        participant.field_values.values_list("field__field_name", "value")
    }
    
    # list of tuples of (intervention id, intervention name, intervention date)
    intervention_data = [
        (intervention.id, intervention.name, intervention_dates_map.get(intervention.id, ""))
        for intervention in study.interventions.order_by("name")
    ]
    # list of tuples of field name, value.
    field_data = [
        (field_id, field_name, participant_fields_map.get(field_name, ""))
        for field_id, field_name
        in study.fields.order_by("field_name").values_list('id', "field_name")
    ]
    
    # dictionary structured for page rendering
    latest_notification_attempt = get_notification_details(
        query_values_for_notification_history(participant.id).first(),
        study.timezone,
        get_survey_names_dict(study)
    )
    
    participant_messages = (
        participant
            .participant_messages
            .prefetch_related("participant__study")
            .order_by("-created_on")
    )

    return render(
        request,
        'participant.html',
        context=dict(
            participant=participant,
            participant_messages=participant_messages,
            study=study,
            intervention_data=intervention_data,
            field_values=field_data,
            notification_attempts_count=participant.archived_events.count(),
            latest_notification_attempt=latest_notification_attempt,
            push_notifications_enabled_for_ios=check_firebase_instance(require_ios=True),
            push_notifications_enabled_for_android=check_firebase_instance(require_android=True),
        )
    )

def query_values_for_notification_history(participant_id) -> ArchivedEventQuerySet:
    return (
        ArchivedEvent.objects
        .filter(participant_id=participant_id)
        .order_by('-created_on')
        .annotate(
            survey_id=F('survey_archive__survey'), survey_version=F('survey_archive__archive_start')
        )
        .values(
            'scheduled_time', 'created_on', 'survey_id', 'survey_version', 'schedule_type', 'status'
        )
    )

def get_survey_names_dict(study: Study):
    survey_names = {}
    for survey in study.surveys.all():
        if survey.name:
            survey_names[survey.id] = survey.name
        else:
            survey_names[survey.id] =\
                ("Audio Survey " if survey.survey_type == 'audio_survey' else "Survey ") + survey.object_id
    
    return survey_names


def get_notification_details(archived_event: Dict, study_timezone: str, survey_names: Dict):
    # Maybe there's a less janky way to get timezone name, but I don't know what it is:
    #  Nah its cool, this might be verbose but handles all the special cases.
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


@require_http_methods(['GET', 'POST'])
@authenticate_researcher_study_access
def schedule_message(request: ResearcherRequest, study_id: int, patient_id: str):
    participant = get_object_or_404(Participant, patient_id=patient_id)
    study = get_object_or_404(Study, pk=study_id)
    # TODO: confirm that participant and study match
    # TODO: check that request.values and request.method work
    form = ParticipantMessageForm(request.values or None, participant=participant)
    if request.method == "GET":
        return render_schedule_message(request, form, participant)
    if not form.is_valid():
        return render_schedule_message(request, form, participant)
    form.save()
    messages.success(
        request,
        f"Your message to participant \"{participant.patient_id}\" was successfully scheduled."
    )
    return redirect(
        easy_url(
            "participant_pages.participant_page",
            study_id=participant.study_id,
            patient_id=participant.patient_id,
        )
    )

  
# FIXME: convert this function from Flask to Django
def render_schedule_message(request, form, participant):
    return render(
        request,
        "participant_message.html",
        context=dict(
            form=form,
            participant=participant,
        )
    )


# FIXME: convert this function from Flask to Django
@require_http_methods(['GET', 'POST'])
@authenticate_researcher_study_access
def cancel_message(request: ResearcherRequest, study_id: int, patient_id: str, participant_message_uuid):
    # TODO: confirm that participant and study match
    with transaction.atomic():
        # Lock to prevent message from being sent while we're cancelling (or cancelling while it's
        # being sent)
        try:
            participant_message = ParticipantMessage.objects.select_for_update().get(
                uuid=participant_message_uuid,
                participant__study__id=study_id,
            )
        except ParticipantMessage.DoesNotExist:
            messages.warning(request, "Sorry, could not find the message specified.")
        else:
            if participant_message.status == ParticipantMessageStatus.sent:
                messages.danger(
                    request,
                    "Sorry, could not cancel because the message was already sent."
                )
            elif participant_message.status == ParticipantMessageStatus.error:
                messages.danger(
                    request,
                    "Sorry, could not cancel because the message status is \"error\" and it may "
                    "have already been sent."
                )
            elif participant_message.status in ParticipantMessageStatus.scheduled:
                if participant_message.status == ParticipantMessageStatus.scheduled:
                    participant_message.status = ParticipantMessageStatus.cancelled
                    participant_message.save(update_fields=["status"])
                messages.success(request, "The message was successfully cancelled.")
            elif participant_message.status in ParticipantMessageStatus.cancelled:
                messages.success(request, "The message was successfully cancelled.")
    return redirect(
        easy_url(
            "participant_pages.participant_page",
            study_id=participant_message.participant.study_id,
            patient_id=participant_message.participant.patient_id,
        )
    )


def format_date_or_none(d: date) -> str:
    # tiny function that broke scanability of the real code....
    return d.strftime(API_DATE_FORMAT) if isinstance(d, date) else ""
