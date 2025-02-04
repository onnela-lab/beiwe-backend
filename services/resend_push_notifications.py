import logging
import uuid
from datetime import datetime, timedelta
from typing import Callable, List

from django.utils import timezone

# do not import from libs.schedules or services.survey_push_natifications !!!
from constants.common_constants import RUNNING_TESTS
from constants.message_strings import MESSAGE_SEND_SUCCESS
from constants.user_constants import IOS_API, IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION
from database.schedule_models import ArchivedEvent, ScheduledEvent
from database.study_models import Study
from database.system_models import GlobalSettings
from database.user_models_participant import Participant, SurveyNotificationReport
from libs.push_notification_helpers import fcm_for_pushable_participants
from libs.sentry import time_warning_data_processing
from libs.utils.participant_app_version_comparison import (is_participants_version_gte_target,
    VersionError)


ParticipantPKs = int
ScheduledEventPK = int

logger = logging.getLogger("push_notifications")
if RUNNING_TESTS:
    logger.setLevel(logging.ERROR)
else:
    logger.setLevel(logging.INFO)

log = logger.info
logw = logger.warning
loge = logger.error
logd = logger.debug


@time_warning_data_processing("Warning: resend logic took over 30 seconds", 30)
def restore_scheduledevents_logic():
    """
    Participants upload a list of uuids of their received notifications, these uuids are stashed in
    SurveyNotificationReport, are sourced from ScheduledEvents, and recorded on ArchivedEvents.
    
    Complex details about how to manipulate ScheduledEvents and ArchivedEvents correctly:
    
    - We exclude Archives from before this feature existed do not have the uuid field populated.
    - This code does not create scheduled events, it reactivates old ScheduledEvents by uuid.
    
    Other Details:
    
    - To inject a "no more than one resend every 30 minutes" we need to add a filtering by last
      updated time ArchivedEvent query, and "touch" all modified archive events when we check them.
    - We have to do an app version check for when uuid-reporting was added this feature, that may e
      subject to change and must be documented.
    - Only participants with an app version that passes the version check will create ArchivedEvents
      with uuids, so only push notifications to known-capable devices will activate resends.
    - Weekly schedules do not require special casing - they get removed from the database after a
      week anyway, and if the two schedule periods overlap then the app merges them.
    """
    
    if GlobalSettings.singleton().push_notification_resend_enabled is None:
        return  # do not run resends if the this is not populated.
    
    # Start time, clear out any problems from the past, Go.
    now = timezone.now()
    disable_resend_on_problem_scheduled_events(now)
    
    pushable_participant_pks = base_resend_logic_participant_query(now)
    log("pushable_participant_pks:", pushable_participant_pks)
    
    # UUIDs a NotificationReports are checked to confirm receipt of a notification.
    update_ArchivedEvents_from_SurveyNotificationReports(pushable_participant_pks, now, log)
    unconfirmed_notification_uuids = get_resendable_uuids(now, pushable_participant_pks)
    log("unconfirmed_notification_uuids:", unconfirmed_notification_uuids)
    
    # re-enable all ScheduledEvents that were not confirmed received.
    query_scheduledevents_updated = ScheduledEvent.objects.filter(
        uuid__in=unconfirmed_notification_uuids,
        # created_on__gte=TOO_EARLY,  # No. We migrate old ScheduledEvents, not archives, to have uuids.
    ).update(
        deleted=False, last_updated=now
    )
    log("query_scheduledevents_updated:", query_scheduledevents_updated)
    
    # mark ArchivedEvents that just forced ScheduledEvent.deleted=False as last_updated=now to block
    # a resend for the next 30 minutes.
    query_archive_update_last_updated = ArchivedEvent.objects.filter(
        uuid__in=unconfirmed_notification_uuids,
        # created_on__gte=TOO_EARLY,  # already filtered out
    ).update(
        last_updated=now
    )
    log("query_archive_update_last_updated:", query_archive_update_last_updated)
    
    # Of the uuids we identified we need to mark any that lack existing ScheduledEvents as
    # unresendable; we do this by clearing their uuid field.
    extant_schedule_uuids = list(
        ScheduledEvent.objects.filter(
            uuid__in=unconfirmed_notification_uuids,
            # created_on__gte=the_beginning_of_time,  # already filtered out
        ).values_list("uuid", flat=True)
    )
    log("extant_schedule_uuids:", extant_schedule_uuids)
    
    unresendable_archive_uuids = list(set(unconfirmed_notification_uuids) - set(extant_schedule_uuids))
    log("unresendable_archive_uuids:", unresendable_archive_uuids)
    
    query_archive_unresendable = ArchivedEvent.objects.filter(
        uuid__in=unresendable_archive_uuids
    ).update(
        uuid=None, last_updated=now
    )
    log("query_archive_unresendable:", query_archive_unresendable)


def disable_resend_on_problem_scheduled_events(now: datetime):
    """ Situations where a ScheduledEvent must be flagged to block resends:
    1) If (all) _Schedules_ on a ScheduledEvent are None - this causes the send push notification
    code to get stuck in a retry loop because creating the ArchivedEvent fails if the schedule is
    ever resurrected by resend logic.
      - I think there is a race condition in updating schedules.
      - There is an oversight in a migration script - can't affect new deployments.
      - Manual push notifications (pre-integrating with resend logic) have this form by design.
      (This also bypassing time-gating logic from the ArchivedEvent's last_updated field, it pushes
      the notification every 6 minutes.) """
    
    bugged_uuids = list(ScheduledEvent.objects.filter(
        weekly_schedule__isnull=True, relative_schedule__isnull=True, absolute_schedule__isnull=True,
        no_resend=False  # don't get anything that is already disabled
    ).values_list("uuid", flat=True))
    if bugged_uuids:
        log(f"Discovered and disabled {len(bugged_uuids)} problemy ScheduledEvents.")
        ArchivedEvent.objects.filter(uuid__in=bugged_uuids).update(last_updated=now)  # tracking...
        ScheduledEvent.objects.filter(uuid__in=bugged_uuids).update(last_updated=now, no_resend=True, deleted=True)


def base_resend_logic_participant_query(now: datetime) -> List[ParticipantPKs]:
    """ Current filter: iOS-only, with the minimum build version. """
    one_week_ago = now - timedelta(days=7)
    
    # base - split off of the heartbeat valid participants query
    pushable_participant_info = list(
        fcm_for_pushable_participants(one_week_ago)
        .filter(participant__os_type=IOS_API)  # only ios
        .values_list(
            "participant_id",
            "participant__os_type",
            "participant__last_version_code",
            "participant__last_version_name",
        )
    )
    
    # complex filters, participant app version and os type
    pushable_participant_pks = []
    for participant_id, os_type, version_code, version_name in pushable_participant_info:
        # I'm injecting an exta check here as a reminder no .... do the thing the message says.
        if os_type != IOS_API:
            raise AssertionError("this code is currently only supposed to be for ios, you to update can_handle_push_notification_resends if you change it.")
        # build must be greater than or equal to 2024.22-or-whatever it turns out to be
        try:
            if is_participants_version_gte_target(
                os_type, version_code, version_name, IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION
            ):
                pushable_participant_pks.append(participant_id)
        except VersionError:
            pass
    
    return pushable_participant_pks


def get_unconfirmed_notification_schedules(
    participant: Participant, excluded_pks: list[ScheduledEventPK]
) -> list[ScheduledEvent]:
    """ We need to send all unconfirmed surveys to along with all other surveys whenever we send a notification. """
    
    if participant.os_type != IOS_API:
        return []
    
    # cribbed from base_resend_logic_participant_query, adapted to participant object,
    try:
        proceed = is_participants_version_gte_target(
            participant.os_type,
            participant.version_code,
            participant.version_name,
            IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION,
        )
        if not proceed:
            return []
    except VersionError:
        return []
    
    # get All the possible resends, skip the excluded ones (already selected).
    # (this _is_ the logic for getting all unconfirmed notifications)
    way_in_the_future = timezone.now() + timedelta(days=365)
    resend_uuids = get_resendable_uuids(way_in_the_future, [participant.pk])
    events = participant.scheduled_events.filter(uuid__in=resend_uuids).exclude(pk__in=excluded_pks)
    return list(events)


def update_ArchivedEvents_from_SurveyNotificationReports(
    participant_pks: List[ParticipantPKs], update_timestamp: datetime, log: Callable
):
    """ Populates confirmed_received and applied on ArchivedEvents and SurveyNotificationReports
    based on the SurveyNotificationReports, sets last_updated. """
    
    # Possible Bug -- there is some condition where we miss a notification report updating an
    #  archived event that is supposed to already be applied.  Fix is to always get all of them and
    #  filter on the update operation. these uuids are unique, and its only active participants.
    #  I don't know what the cause was, best guess is database state updating between queries?
    #  We can still track paths of this occurring by looking at last_updated and created_on values.
    
    # Get all notification_report_pk and uuid pairs.
    notification_report_pk__uuid = list(SurveyNotificationReport.objects.filter(
        participant_id__in=participant_pks,
        # applied=False,  # see bug comment above.
    ).values_list(
        "pk", "notification_uuid"
    ))
    
    if not notification_report_pk__uuid:
        # this is log and return exist to assist debugging, virtually unreachable on a live server
        log("no notification reports found to update.\n--")
        return 
    
    log(f"notification_report_pk__uuid: {notification_report_pk__uuid}")
    
    # ArchivedEvents matching those uuids to `confirmed_received=True` and `last_updated` to now.
    query_update_archive_confirm_received = ArchivedEvent.objects.filter(
        uuid__in=[uuid for _, uuid in notification_report_pk__uuid],
        # created_on__gte=TOO_EARLY,  # no, we can have overlap on users that update early... probably
        confirmed_received=False,     # see bug comment above; reduces db load and lets us track.
    ).update(
        confirmed_received=True, last_updated=update_timestamp,
    )
    log(f"query_archive_confirm_received: {query_update_archive_confirm_received}")
    
    # then update NotificationReports `applied` to True so we only do it once.
    query_update_notification_report = SurveyNotificationReport.objects.filter(
        pk__in=[pk for pk, _ in notification_report_pk__uuid],
        applied=False,          # see bug comment above; reduces db load and lets us track.
    ).update(
        applied=True, last_updated=update_timestamp
    )
    log(f"query_notification_report: {query_update_notification_report}")


def get_resendable_uuids(now: datetime, pushable_participant_pks: List[ParticipantPKs]) -> List[uuid.UUID]:
    """ Get the uuids of relevant archives. This includes a per-study timeout value for how frequently
    to resend, and a filter by last updated time. aoeuaoeu eua eou.a aoeui .,..uaoeuaoeuaouiaoeu aoeu oaeu a """
    
    # TOO_EARLY is populated AFTER the first run that regenerates all schedules in a periodic task
    # - only archived events created after this time are considered for resends.
    TOO_EARLY = GlobalSettings.singleton().push_notification_resend_enabled
    
    # retired just means this flag has been set to True for some reason
    retired_uuids = set(ScheduledEvent.objects.filter(no_resend=True).values_list("uuid", flat=True))
    
    # Now we can filter ArchivedEvents to get all that remain unconfirmed.
    uuid_info = list(
        ArchivedEvent.objects.filter(
            created_on__gt=TOO_EARLY,                     # created after the earliest possible time,
            # last_updated__lte=thirty_minutes_ago,       # originally hardcoded.
            status=MESSAGE_SEND_SUCCESS,                  # that should have been received,
            participant_id__in=pushable_participant_pks,  # from relevant participants,
            confirmed_received=False,                     # that are not confirmed received,
            uuid__isnull=False,                           # and have uuids.
        # well this failed in preduction....
        # ).exclude(
            # uuid__in=retired_uuids,                        # exclude schedules that have been retired.
        ).values_list(
            "uuid",
            "last_updated",
            "participant__study_id",
        )
    )
    # probably due to too many uuids, excluding these from the query directly Simple Doesn't Work,
    # so I guess to exclude it in python?
    uuid_info = [info for info in uuid_info if info[0] not in retired_uuids]
    
    log(f"found {len(uuid_info)} ArchivedEvents to check.")
    
    # handle _some_ off-by-X-minutes issues:
    # - Clear seconds and microseconds on now to cause all values in the current minute as resendable.
    # - true off-by-6-minutes requires a `seconds - (seconds % 6)`` operation....
    # Not fully handling that is ok because it only occurs when push notification went out slow?
    now_ish = now.replace(second=0, microsecond=0)
    # print("periodicity:", list(Study.objects.values_list("device_settings__resend_period_minutes", flat=True)))
    
    # Every study has a different timeout value, but we exclude 0.
    # If the last updated timestamp on the archive is before this value, we resend.
    study_resend_timeouts = {
        pk: now_ish - timedelta(minutes=minutes)
        for pk, minutes in Study.objects.values_list("pk", "device_settings__resend_period_minutes")
        if minutes > 0
    }
    
    # filter by last updated time lte the studies timeout value, uniqueify, listify, return.
    uuids = []
    for a_uuid, last_updated, study_id in uuid_info:
        timeout = study_resend_timeouts.get(study_id, None)
        # from constants.common_constants import DEV_TIME_FORMAT3
        # print("study's timeout:", timeout.strftime(DEV_TIME_FORMAT3) if timeout else timeout)
        # print("last_updated:   ", last_updated.strftime(DEV_TIME_FORMAT3))
        # print("timeout and last_updated <= timeout:", timeout and last_updated <= timeout)
        # print()
        if timeout and last_updated <= timeout:
            uuids.append(a_uuid)
    
    # deduplicate uuids
    uuids = list(set(uuids))
    log(f"found {len(uuids)} ArchivedEvents to resend.")
    return uuids
