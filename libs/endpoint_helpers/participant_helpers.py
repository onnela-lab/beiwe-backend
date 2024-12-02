from datetime import date, datetime, tzinfo
from typing import Dict

from django.contrib import messages
from django.core.paginator import Paginator
from django.db.models import F
from django.shortcuts import render

from constants.action_log_messages import HEARTBEAT_PUSH_NOTIFICATION_SENT
from constants.common_constants import API_DATE_FORMAT
from constants.message_strings import PARTICIPANT_LOCKED
from constants.user_constants import DATA_DELETION_ALLOWED_RELATIONS
from database.schedule_models import ArchivedEvent
from database.study_models import Study
from database.user_models_participant import Participant
from libs.firebase_config import check_firebase_instance
from libs.internal_types import ArchivedEventQuerySet, ResearcherRequest
from libs.utils.http_utils import niceish_iso_time_format


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
        in study.fields.order_by("field_name").values_list("id", "field_name")
    ]
    
    uuids_to_received_times = {}
    notification = query_values_for_notification_history(participant.id).first()
    if notification and (a_uuid:= notification["uuid"]):
        if a_confirmation := participant.notification_reports.filter(notification_uuid=a_uuid).first():
            uuids_to_received_times = {a_uuid: a_confirmation.created_on}
    
    # dictionary structured for page rendering - we are not showing heartbeat notifications here.
    latest_notification_attempt = notification_details_archived_event(
        notification,
        study.timezone,
        get_survey_names_dict(study),
        uuids_to_received_times,
    )
    
    conditionally_display_locked_message(request, participant)
    study_interventions_exists = study.interventions.exists()
    study_fields_exists = study.fields.exists()
    relation = request.session_researcher.get_study_relation(study.id)
    can_delete = request.session_researcher.site_admin or relation in DATA_DELETION_ALLOWED_RELATIONS
    
    return render(
        request,
        "participant.html",
        context=dict(
            participant=participant,
            study=study,
            intervention_data=intervention_data,
            field_values=field_data,
            notification_attempts_count=participant.archived_events.count(),
            latest_notification_attempt=latest_notification_attempt,
            push_notifications_enabled_for_ios=check_firebase_instance(require_ios=True),
            push_notifications_enabled_for_android=check_firebase_instance(require_android=True),
            study_easy_enrollment=study.easy_enrollment,
            participant_easy_enrollment=participant.easy_enrollment,
            locked=participant.is_dead,
            can_delete=can_delete,
            study_timezone=participant.study.timezone,
            study_interventions_exists=study_interventions_exists,
            study_fields_exists=study_fields_exists,
        )
    )


def get_survey_names_dict(study: Study):
    survey_names = {}
    for survey in study.surveys.all():
        if survey.name:
            survey_names[survey.id] = survey.name
        else:
            survey_names[survey.id] =\
                ("Audio Survey " if survey.survey_type == "audio_survey" else "Survey ") + survey.object_id
    
    return survey_names


def query_values_for_notification_history(participant_id) -> ArchivedEventQuerySet:
    return (
        ArchivedEvent.objects
        .filter(participant_id=participant_id)
        .order_by("-created_on")
        .annotate(
            survey_id=F("survey_archive__survey"), survey_version=F("survey_archive__archive_start")
        )
        .values(
            "scheduled_time", "created_on", "survey_id", "survey_version", "schedule_type",
            "status", "survey_archive__survey__deleted", "uuid", "confirmed_received",
        )
    )


def notification_details_archived_event(
    archived_event: Dict, study_timezone: tzinfo, survey_names: Dict, uuids_to_received_times: Dict
) -> Dict[str, str]:
    """ assembles the details of a notification attempt for display on a page. """
    
    if archived_event is None:
        return {}
    survey_version = archived_event["survey_version"].strftime("%Y-%m-%d %-I:%M %p")
    hover_text = f"Survey version: {survey_version}"
    
    a_uuid = archived_event["uuid"]
    if a_uuid:
        hover_text += f"\nPush Notification ID: {str(a_uuid)}"
    
    # this time is NOT the time it was received, it is the time the device first hit an endpoint
    # and included a record of receiving this notification.
    confirmed_time = uuids_to_received_times[a_uuid] if a_uuid in uuids_to_received_times else None
    
    if archived_event["confirmed_received"] or confirmed_time:
        css = "tableRowReceived"
        status = "Received"
    elif archived_event["status"] == "success":  # we don't even show the real status, too messy.
        status = "Sent"
        css = "tableRowPending"
    else:
        status = "Failed"
        css = "tableRowError"
    
    return {
        "scheduled_time": niceish_iso_time_format(archived_event["scheduled_time"], study_timezone),
        "attempted_time": niceish_iso_time_format(archived_event["created_on"], study_timezone),
        "confirmed_time": niceish_iso_time_format(confirmed_time, study_timezone),
        "survey_name": survey_names[archived_event["survey_id"]],
        "survey_id": archived_event["survey_id"],
        "survey_deleted": archived_event["survey_archive__survey__deleted"],
        "schedule_type": archived_event["schedule_type"].title(),
        "status": status,
        "css_class": css,
        "hover_text": hover_text,
    }


def notification_details_heartbeat(
    heartbeat_timestamp: datetime, study_timezone: tzinfo) -> Dict[str, str]:
    return {
        "scheduled_time": "-",
        "attempted_time": niceish_iso_time_format(heartbeat_timestamp, study_timezone),
        "survey_name": "-",
        "survey_id": "-",
        "survey_version": "-",
        "schedule_type": "Inactivity Notification",
        "status": "Sent",
        "css_class": "tableRowPending",
        "confirmed_time": "-",
        "hover_text": "Inactivity Notification",
        # "survey_deleted" # we don't actually need to include this.
    }


def format_date_or_none(d: date) -> str:
    # tiny function that broke scanability of the real code....
    return d.strftime(API_DATE_FORMAT) if isinstance(d, date) else ""


def conditionally_display_locked_message(request: ResearcherRequest, participant: Participant):
    """ Displays a warning message if the participant is locked. """
    if participant.is_dead:
        messages.warning(request, PARTICIPANT_LOCKED.format(patient_id=participant.patient_id))


def get_heartbeats_query(participant: Participant, archived_events_page: Paginator, page_number: int):
    """ Using the elements in the archived pages, determine the bounds for the query of heartbeats,
    and then construct and return that query. """
    
    # tested, this does return the size of the page
    count = len(archived_events_page.object_list)
    
    if page_number == 1 and count < 25:
        # fewer than 25 notifications on the first page means that is all of them. So, get all the
        # heartbeats too. (this also detects and handles the case of zero total survey
        # notifications)
        heartbeat_query = participant.action_logs.filter(action=HEARTBEAT_PUSH_NOTIFICATION_SENT)
    elif page_number == 1 and count == 25:
        # if there are exactly 25 notifications on the first page then we want everything after
        # (greater than) the last notification on the page, no latest (most recent) bound).
        heartbeat_query = participant.action_logs.filter(
            timestamp__gte=archived_events_page[-1]["created_on"],
            action=HEARTBEAT_PUSH_NOTIFICATION_SENT
        )
    elif count < 25:
        # any non-full pages that are not the first page = get all heartbeats before the top (most
        # recent) notification on the page with no earliest (most in-the-past) bound.
        heartbeat_query = participant.action_logs.filter(
            timestamp__lte=archived_events_page[0]["created_on"],
            action=HEARTBEAT_PUSH_NOTIFICATION_SENT,
        )
    elif count == 25:
        # if there are exactly 25 notifications and we are not on the first page, then we bound it
        # by the first and last notifications... but that leaves out heartbeats between pages... but
        # that's both transient and rare? Solving this requires an extra queries and is hard so
        # unless someone complains we just will ignore this.
        # (we would need the date of the notification that came after the top (most recent)
        # notification in our list, and then use that as the upper (most recent) bound.)
        heartbeat_query = participant.action_logs.filter(
            action=HEARTBEAT_PUSH_NOTIFICATION_SENT,
            timestamp__range=(archived_events_page[0]["created_on"], archived_events_page[-1]["created_on"])
        )
    else:
        raise Exception("shouldn't that cover everything?")
    
    return heartbeat_query
