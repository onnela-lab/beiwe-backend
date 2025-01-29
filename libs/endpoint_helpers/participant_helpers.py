# trunk-ignore-all(bandit/B101)
from datetime import date, datetime, tzinfo
from typing import Dict, List, Tuple
from uuid import UUID

import bleach
from django.contrib import messages
from django.core.paginator import Paginator
from django.db.models import F, Manager
from django.shortcuts import render

from constants.action_log_messages import HEARTBEAT_PUSH_NOTIFICATION_SENT
from constants.common_constants import (API_DATE_FORMAT, DT_12HR_N_TZ_N_SEC_N_PAREN,
    DT_24HR_W_TZ_W_SEC_N_PAREN, T_24HR_W_TZ_N_SEC_N_PAREN)
from constants.message_strings import PARTICIPANT_LOCKED
from constants.user_constants import DATA_DELETION_ALLOWED_RELATIONS
from database.schedule_models import ArchivedEvent
from database.study_models import Study
from database.user_models_participant import Participant, ParticipantActionLog
from libs.firebase_config import check_firebase_instance
from libs.internal_types import ArchivedEventQuerySet, ResearcherRequest
from libs.utils.http_utils import (compact_iso_time_format, more_compact_iso_time_format,
    niceish_iso_time_format, null_time_format)


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
        scheduled_formatter=niceish_iso_time_format,
        attempt_formatter=niceish_iso_time_format,
        confirmed_formatter=niceish_iso_time_format,
    )
    
    conditionally_display_locked_message(request, participant)
    study_interventions_exists = study.interventions.exists()
    study_fields_exists = study.fields.exists()
    relation = request.session_researcher.get_study_relation(study.id)
    can_delete = request.session_researcher.site_admin or relation in DATA_DELETION_ALLOWED_RELATIONS
    
    enable_interventions = check_firebase_instance(require_ios=True) or check_firebase_instance(require_android=True)
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
            enable_interventions=enable_interventions,
            study_easy_enrollment=study.easy_enrollment,
            participant_easy_enrollment=participant.easy_enrollment,
            locked=participant.is_dead,
            can_delete=can_delete,
            study_timezone=participant.study.timezone,
            study_interventions_exists=study_interventions_exists,
            study_fields_exists=study_fields_exists,
        )
    )


def get_survey_names_dict(study: Study) -> Dict[int, str]:
    survey_names = {}
    fields = ("pk", "name", "survey_type", "object_id")
    for survey_id, survey_name, survey_type, obj_id in study.surveys.values_list(*fields):
        if survey_name:
            survey_names[survey_id] = survey_name
        else:
            survey_names[survey_id] =\
                ("Audio Survey " if survey_type == "audio_survey" else "Survey ") + obj_id
    
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
            "status", "survey_archive__survey__deleted", "uuid", "confirmed_received", "was_resend"
        )
    )


def format_date_or_none(d: date) -> str:
    # tiny function that broke scanability of the real code....
    return d.strftime(API_DATE_FORMAT) if isinstance(d, date) else ""


def conditionally_display_locked_message(request: ResearcherRequest, participant: Participant):
    """ Displays a warning message if the participant is locked. """
    if participant.is_dead:
        messages.warning(request, PARTICIPANT_LOCKED.format(patient_id=participant.patient_id))


def get_heartbeats_query(participant: Participant, archived_events_page: Paginator, page_number: int) -> Manager[ParticipantActionLog]:
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


def notification_details_archived_event(
    archived_event: Dict,
    study_timezone: tzinfo,
    survey_names: Dict,
    uuids_to_received_times: Dict,
    scheduled_formatter: callable,
    attempt_formatter: callable,
    confirmed_formatter: callable,
) -> Dict[str, str]:
    """ Assembles the details of a notification attempt for display on a page. """
    
    if archived_event is None:
        return {}
    
    survey_version = archived_event["survey_version"].strftime(DT_12HR_N_TZ_N_SEC_N_PAREN)
    hover_text = f"Survey version: {survey_version}"
    
    a_uuid = archived_event["uuid"]  # cannot be inlined, we need the None
    if a_uuid:
        hover_text += f"\nPush Notification ID: {str(a_uuid)}"
    
    # this time is NOT the time it was received, it is the time the device first hit an endpoint
    # and included a record of receiving this notification.
    confirmed_time = uuids_to_received_times[a_uuid] if a_uuid in uuids_to_received_times else None
    
    # there is a css class that we target
    if archived_event["confirmed_received"] or confirmed_time:
        css = "tableRowReceived"
        status = "Received"
    elif archived_event["status"] == "success":  # we don't even show the real status, too messy.
        status = "Sent"
        css = "tableRowPending"
    else:
        status = "Failed"
        css = "tableRowError"
    
    schedule_type = archived_event["schedule_type"].title()
    if archived_event["was_resend"]:  # tack resend onto schedule type
        schedule_type += " (Resend)"
    
    return {
        "scheduled_time": scheduled_formatter(archived_event["scheduled_time"], study_timezone),  # participant page
        "attempted_time": attempt_formatter(archived_event["created_on"], study_timezone),
        "confirmed_time": confirmed_formatter(confirmed_time, study_timezone) or "-",
        "survey_name": survey_names[archived_event["survey_id"]],
        "survey_id": archived_event["survey_id"],
        "survey_deleted": archived_event["survey_archive__survey__deleted"],
        "schedule_type": schedule_type,
        "status": status,
        "css_class": css,
        "hover_text": hover_text,
    }


def convert_to_page_expectations(
    all_notifications: List[Dict],
    tz: tzinfo,
    survey_names: Dict[int, str],
    uuids_to_received_time: Dict[UUID, datetime],
):
    ret = []
    prior_sched_time = None
    run_of_notifications: List[Dict] = []
    run_of_heartbeats: List[datetime] = []
    
    # these functions iterate to a next run of notifications or heartbeats
    def terminate_run_of_notifications():
        if run_of_notifications:
            ret.extend(frontend_details_for_history_run_from_archived_events(
                run_of_notifications, tz, survey_names, uuids_to_received_time))
            run_of_notifications.clear()
    def terminate_run_of_heartbeats():
        if run_of_heartbeats:
            ret.append(message_from_heartbeat_list(run_of_heartbeats, tz))
            run_of_heartbeats.clear()
    
    # track the runs of heartbeats, get their times, create a nice messagce for them as a single row.
    for maybe_notification_maybe_heartbeat in all_notifications:
        if isinstance(maybe_notification_maybe_heartbeat, dict):
            terminate_run_of_heartbeats()  # its a notification dict.
            
            # old notification scheduled time
            if maybe_notification_maybe_heartbeat['scheduled_time'] == prior_sched_time:
                run_of_notifications.append(maybe_notification_maybe_heartbeat)  # its a notification dict.
            else:  # new notification scheduled time
                prior_sched_time = maybe_notification_maybe_heartbeat['scheduled_time']
                terminate_run_of_notifications()  # its a NEW notification dict.
                run_of_notifications.append(maybe_notification_maybe_heartbeat)
        else:
            terminate_run_of_notifications()
            run_of_heartbeats.append(maybe_notification_maybe_heartbeat)  # its a heartbeat.
    
    # have to run these to clear out remaining lists.
    terminate_run_of_notifications()
    terminate_run_of_heartbeats()
    return ret


def frontend_details_for_history_run_from_archived_events(
    archived_events: List[Dict], study_timezone: tzinfo, survey_names: Dict, uuids_to_received_times: Dict
) -> List[Dict[str, str]]:
    """ When listing on the notification history page:
    - Iteration is in reverse chronological order by _schedule_ time.
    - Need to sort by survey_id - in place.
    - Only the most recent notification attempt should say "Received", priors should say "Sent"
    - Confirmed time is - if not received, or the time it was received if it was received.
    """
    ret = []
    scheduled_time = archived_events[0]["scheduled_time"]
    scheduled_time_repr = gratuitoussmallcaps(
        more_compact_iso_time_format(archived_events[0]["scheduled_time"], study_timezone)
    )
    # scheduled_time_repr = '<span class="text-mono">' + scheduled_time_repr + "</span>"
    archived_events.sort(key=lambda x: x["survey_id"])
    
    # assemble frontend details
    prev_survey_id = None
    for event in archived_events:
        assert scheduled_time == event["scheduled_time"], "all events must have the same scheduled time"
        notification_details = notification_details_archived_event(
            event,
            study_timezone,
            survey_names,
            uuids_to_received_times,
            scheduled_formatter=null_time_format,
            attempt_formatter=compact_iso_time_format,
            confirmed_formatter=compact_iso_time_format,
        )
        
        title = f"{scheduled_time_repr}"
        if prev_survey_id == notification_details["survey_id"]:
            # if survey did not change ...
            if notification_details["status"] == "Received":
                notification_details["status"] = "Sent"
                notification_details["confirmed_time"] = "-"
        else:
            title += f' - <i>{bleach.clean(notification_details["survey_name"])}</i><br>'
            ret.append( (title, ) ) # a container of length 1 is the signal that it is a ~header-like row
            prev_survey_id = notification_details["survey_id"]
        
        ret.append(notification_details)
    
    #to inject a bit of logic here to coditionally display the button on only some lines we would
    # need to track the survey ID for these items, we are not currently doing that.
    # ret[-(len(archived_events))]["show_resend_button"] = True
    return ret


def message_from_heartbeat_list(heartbeat_timestamps: List[datetime], tz: tzinfo) -> Tuple[str]:
    """ Needs to return an item with length 1 containing a string. """
    
    num_beats = len(heartbeat_timestamps)
    d1 = heartbeat_timestamps[0].astimezone(tz)
    if num_beats == 1:
        t1 = gratuitoussmallcaps(d1.strftime(DT_24HR_W_TZ_W_SEC_N_PAREN))
        ret = f"One keepalive notification sent to the device at {t1}."
    else:
        d2 = heartbeat_timestamps[-1].astimezone(tz)
        if d1.date() == d2.date():
            t1 = gratuitoussmallcaps(d1.strftime(T_24HR_W_TZ_N_SEC_N_PAREN))
            t2 = gratuitoussmallcaps(d2.strftime(T_24HR_W_TZ_N_SEC_N_PAREN))
            day = d2.date().isoformat()
            # t2 actually comes before t1
            ret = f"{num_beats} keepalive notifications sent to the device on {day} between {t2} and {t1}."
        else:
            t1 = gratuitoussmallcaps(d1.strftime(DT_24HR_W_TZ_W_SEC_N_PAREN))
            t2 = gratuitoussmallcaps(d2.strftime(DT_24HR_W_TZ_W_SEC_N_PAREN))
            # t2 actually comes before t1
            ret = f"{num_beats} keepalive notifications sent to the device between {t2} and {t1}."
    print(ret)
    return (ret, )


def gratuitoussmallcaps(text: str) -> str:
    return '<span style="font-variant: small-caps; font-weight: 400; font-style: italic;">' + text.lower() + '</span>'
