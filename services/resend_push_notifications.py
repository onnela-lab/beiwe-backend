import logging
import uuid
from datetime import datetime, timedelta
from typing import Callable, List

from django.utils import timezone

# do not import from libs.schedules
from constants.common_constants import RUNNING_TESTS
from constants.message_strings import MESSAGE_SEND_SUCCESS
from constants.user_constants import IOS_API, IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION
from database.schedule_models import ArchivedEvent, ScheduledEvent
from database.study_models import Study
from database.system_models import GlobalSettings
from database.user_models_participant import SurveyNotificationReport
from libs.push_notification_helpers import fcm_for_pushable_participants
from libs.utils.participant_app_version_comparison import is_participants_version_gte_target


ParticipantPKs = int


logger = logging.getLogger("push_notifications")
if RUNNING_TESTS:
    logger.setLevel(logging.ERROR)
else:
    logger.setLevel(logging.INFO)

log = logger.info
logw = logger.warning
loge = logger.error
logd = logger.debug


def undelete_events_based_on_lost_notification_checkin():
    """
    Participants upload a list of uuids of their received notifications, these uuids are stashed in
    SurveyNotificationReport, are sourced from ScheduledEvents, and recorded on ArchivedEvents.
    
    Complex details about how to manipulate ScheduledEvents and ArchivedEvents correctly:
    
    - We exclude Archives from before this feature existed do not have the uuid field populated.
    - This code does not create scheduled events, it reactivates old ScheduledEvents by uuid.
    - If ScheduledEvents get recalculated we do lose that uuid, and would get stuck in a loop trying
      to find the matching ScheduledEvent by uuid from an old ArchivedEvent. We solve that by
      identifying these non-match-uuids and clearing the uuid field. This will fully retire the
      ArchivedEvent from the resend logic because we exclude those to start with.
        FIXME: change this above point not how we do it, let's add a give-up flag and preserve uuids.
    - Note that this will create a "new" ScheduledEvent "in the past", forcing a send on the next
      push notification task, bypassing our time-gating logic from the ArchivedEvent's last_updated
      field.
    
    Other Details:
    
    - To inject a "no more than one resend every 30 minutes" we need to add a filtering by last
      updated time ArchivedEvent query, and "touch" all modified archive events when we check them.
    - We have to do an app version check for when uuid-reporting was added this feature, that may e
      subject to change and must be documented.
    - Only participants with an app version that passes the version check will create ArchivedEvents
      with uuids, so only push notifications to known-capable devices will activate resends.
    - Weekly schedules do not require special casing - they get removed from the database after
      a week anyway, and if the two schedule periods overlap then the app merges them.
    """
    now = timezone.now()
    
    #FIXME: this solution where we clear the Nones should be replaced with a real db value that we
    #exclude on in order to have the schedules for manual resends not trigger this..... because we
    #want the manual resends to work like this and we simply can't be clearing the uuid, its how we
    #track stuff.
    
    # We have to clear out a somewhat theoretical assumption violation where the _Schedule_ on a
    # ScheduledEvent has been deleted. This _shouldn't_ happen, but it is possible at least due to a
    # potential race condition in updating schedules, and there there is an oversight in a migration
    # script, and if we in-the-future allow manual push notifications to resend then it will cause
    # this, AND I manually created the condition accidentally in testing.  The outcome isn't a bug
    # here, but it causes an endless retry loop because an ArchivedEvent can no longer be
    # instantiated from that scheduled event.
    bugged_uuids = list(ScheduledEvent.objects.filter(
        weekly_schedule__isnull=True, relative_schedule__isnull=True, absolute_schedule__isnull=True,
        uuid__isnull=False  # don't get anything with uuids
    ).values_list("uuid", flat=True))
    if bugged_uuids:
        ArchivedEvent.objects.filter(uuid__in=bugged_uuids).update(last_updated=now, uuid=None)
        ScheduledEvent.objects.filter(uuid__in=bugged_uuids).update(last_updated=now, uuid=None, deleted=True)
    
    # OK. Now we can move on with our lives. We start with the common query....
    
    pushable_participant_pks = base_resend_logic_participant_query(now)
    log("pushable_participant_pks:", pushable_participant_pks)
    
    # sets last_updated on ArchivedEvents, they are excluded.
    update_ArchivedEvents_from_SurveyNotificationReports(pushable_participant_pks, now, log)
    
    # We have to do some filtering/processing to identify the uuids we can resend.
    unconfirmed_notification_uuids = get_resendable_uuids(now, pushable_participant_pks)
    log("unconfirmed_notification_uuids:", unconfirmed_notification_uuids)
    
    # re-enable all ScheduledEvents that were not confirmed received.
    query_scheduledevents_updated = ScheduledEvent.objects.filter(
        uuid__in=unconfirmed_notification_uuids,
        # created_on__gte=TOO_EARLY,  # No. We migrate old ScheduledEvents to have uuids.
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
        # build must be greater than or equal to 2024.22
        if is_participants_version_gte_target(
            os_type, version_code, version_name, IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION
        ):
            pushable_participant_pks.append(participant_id)
    
    return pushable_participant_pks


def update_ArchivedEvents_from_SurveyNotificationReports(
    participant_pks: List[ParticipantPKs], update_timestamp: datetime, log: Callable
):
    """ Populates confirmed_received and applied on ArchivedEvents and SurveyNotificationReports
    based on the SurveyNotificationReports, sets last_updated. """
    
    # TOO_EARLY is populated as time of deploy that introduced this feature, resend logic
    # only operates on ArchivedEvents created after that.
    TOO_EARLY = GlobalSettings.get_singleton_instance()\
        .earliest_possible_time_of_push_notification_resend
    
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
        created_on__gte=TOO_EARLY,    # created after the earliest possible allowed time.
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
    
    # TOO_EARLY in populated as time of deploy that introduced this feature.
    TOO_EARLY = GlobalSettings.get_singleton_instance()\
        .earliest_possible_time_of_push_notification_resend
    
    # Now we can filter ArchivedEvents to get all that remain unconfirmed.
    uuid_info = list(
        ArchivedEvent.objects.filter(
            created_on__gte=TOO_EARLY,                    # created after the earliest possible time,
            # last_updated__lte=thirty_minutes_ago,       # originally hardcoded.
            status=MESSAGE_SEND_SUCCESS,                  # that should have been received,
            participant_id__in=pushable_participant_pks,  # from relevant participants,
            confirmed_received=False,                     # that are not confirmed received,
            uuid__isnull=False,                           # and have uuids.
        ).values_list(
            "uuid",
            "last_updated",
            "participant__study_id",
        )
    )
    
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
