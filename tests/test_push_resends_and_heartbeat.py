# trunk-ignore-all(ruff/B018)
import uuid
from datetime import datetime, timedelta
from typing import Tuple
from unittest.mock import MagicMock, patch

import time_machine
from dateutil.tz import gettz
from django.utils import timezone
from firebase_admin.messaging import (QuotaExceededError, SenderIdMismatchError,
    ThirdPartyAuthError, UnregisteredError)

from constants.message_strings import DEFAULT_HEARTBEAT_MESSAGE
from constants.user_constants import (ACTIVE_PARTICIPANT_FIELDS, ANDROID_API, IOS_API,
    IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION)
from database.common_models import TimestampedModel, UtilityModel
from database.schedule_models import (AbsoluteSchedule, ArchivedEvent, RelativeSchedule,
    ScheduledEvent, WeeklySchedule)
from database.survey_models import Survey
from database.system_models import GlobalSettings
from database.user_models_participant import (Participant, ParticipantFCMHistory,
    SurveyNotificationReport)
from services.celery_push_notifications import (create_heartbeat_tasks, get_surveys_and_schedules,
    heartbeat_query, undelete_events_based_on_lost_notification_checkin)
from tests.common import CommonTestCase


class TestHeartbeatQuery(CommonTestCase):
    # this test class relies on behavior of the FalseCeleryApp class. Specifically, FalseCeleryApps
    # immediately run the created task synchronously, e.g. calls through safe_apply_async simply run
    # the target function on the same thread completely bypassing Celery.
    
    @property
    def default_participant_response(self):
        # reference output for the default particpiant of the output of the heartbeat query
        return [
            (
                self.default_participant.id,
                self.default_participant.fcm_tokens.first().token,
                ANDROID_API,
                DEFAULT_HEARTBEAT_MESSAGE,
            )
        ]
    
    def test_query_no_participants(self):
        self.assertEqual(Participant.objects.all().count(), 0)
        self.assertEqual(len(heartbeat_query()), 0)
    
    def test_query_one_invalid_participant(self):
        self.using_default_participant()
        self.assertEqual(len(heartbeat_query()), 0)
    
    def test_query_deleted_participant(self):
        self.set_working_heartbeat_notification_fully_valid
        self.assertEqual(Participant.objects.all().count(), 1)
        self.assertEqual(len(heartbeat_query()), 1)
        self.default_participant.update(deleted=True)
        self.assertEqual(len(heartbeat_query()), 0)
    
    def test_query_no_fcm_token(self):
        self.set_working_heartbeat_notification_fully_valid
        self.default_participant.fcm_tokens.all().delete()
        self.assertEqual(len(heartbeat_query()), 0)
    
    def test_deleted_hidden_study(self):
        self.set_working_heartbeat_notification_fully_valid
        self.default_study.update(deleted=True)
        self.assertEqual(len(heartbeat_query()), 0)
    
    def test_manually_stopped_study(self):
        self.set_working_heartbeat_notification_fully_valid
        self.default_study.update(manually_stopped=True)
        self.assertEqual(len(heartbeat_query()), 0)
    
    def test_end_date_in_past(self):
        self.set_working_heartbeat_notification_fully_valid
        # we don't need to test exactly whether timezone crap is functional we already have tests for that.
        self.default_study.update(end_date=timezone.now() - timedelta(days=10))
        self.assertEqual(len(heartbeat_query()), 0)
    
    def test_query_fully_valid(self):
        self.set_working_heartbeat_notification_fully_valid
        self.assertEqual(len(heartbeat_query()), 1)
    
    def test_query_ratelimits(self):
        # make sure the heartbeat won't be sent if the last notification was too recent.
        self.set_working_heartbeat_notification_fully_valid
        self.default_participant.update(last_heartbeat_notification=timezone.now())
        self.assertEqual(len(heartbeat_query()), 0)
        self.default_participant.update(last_heartbeat_notification=timezone.now() - timedelta(minutes=50))
        self.assertEqual(len(heartbeat_query()), 0)
        self.default_participant.update(last_heartbeat_notification=timezone.now() - timedelta(minutes=70))
        self.assertEqual(len(heartbeat_query()), 1)
    
    # rewrote to use a last_heartbeat_checkin field, test no longer valid
    # def test_recent_app_heartbeat_disables_notifications(self):
    #     self.set_working_heartbeat_notification_fully_valid
    #     app_heartbeat = AppHeartbeats.create(self.default_participant, timezone.now())
    #     self.assertEqual(len(heartbeat_query()), 0)
    #     app_heartbeat.update(timestamp=timezone.now() - timedelta(minutes=50))
    #     self.assertEqual(len(heartbeat_query()), 0)
    #     app_heartbeat.update(timestamp=timezone.now() - timedelta(minutes=70))
    #     self.assertEqual(len(heartbeat_query()), 1)
    
    def test_study_configurable_heartbeat_interval(self):
        self.set_working_heartbeat_notification_fully_valid
        now = timezone.now()
        self.default_study.device_settings.update(heartbeat_timer_minutes=30)
        self.default_participant.update(last_upload=now - timedelta(minutes=5))
        self.assertEqual(len(heartbeat_query()), 0)
        self.default_participant.update(last_upload=now - timedelta(minutes=31))
        self.assertEqual(len(heartbeat_query()), 1)
    
    def test_one_minute_offset(self):
        self.set_working_heartbeat_notification_fully_valid
        now = timezone.now()
        self.default_study.device_settings.update(heartbeat_timer_minutes=30)
        self.default_participant.update(last_upload=now - timedelta(minutes=28))
        self.assertEqual(len(heartbeat_query()), 0)
        # this will one will find a valid push notification because we offset the time my 1 minute
        # to fix compounding off-by-6-minute time bugs.
        self.default_participant.update(last_upload=now - timedelta(minutes=29))
        self.assertEqual(len(heartbeat_query()), 1)
        self.default_participant.update(last_upload=now - timedelta(minutes=30))
        self.assertEqual(len(heartbeat_query()), 1)
    
    def test_study_configurable_heartbeat_message(self):
        self.set_working_heartbeat_notification_fully_valid
        self.default_study.device_settings.update(heartbeat_message="test message")
        self.assertEqual(len(heartbeat_query()), 1)
        self.assertEqual(heartbeat_query()[0][3], "test message")
    
    def test_query_each_every_active_field_tautology(self):
        self.set_working_push_notification_basices
        prior_event_time = timezone.now() - timedelta(minutes=61)  # e.g. send the notification
        
        # test the datetime active participant fields
        for field_name in ACTIVE_PARTICIPANT_FIELDS:
            if field_name == "permanently_retired":
                continue
            if not hasattr(self.default_participant, field_name):
                raise ValueError(f"Participant does not have field {field_name}")
            
            self.default_participant.update_only(**{field_name: prior_event_time})
            self.assertEqual(len(heartbeat_query()), 1)
            self.default_participant.update_only(**{field_name: None})
        
        # assert that permanently_retired overrides all other fields
        self.default_participant.update_only(permanently_retired=True)
        self.assertEqual(len(heartbeat_query()), 0)
        
        # and then test 0 if all of them are None
        self.default_participant.update_only(
            **{field_name: None for field_name in ACTIVE_PARTICIPANT_FIELDS if field_name != "permanently_retired"},
            permanently_retired=True,
        )
        self.assertEqual(len(heartbeat_query()), 0)
    
    def test_query_fcm_unregistered(self):
        now = timezone.now()
        self.set_working_heartbeat_notification_fully_valid
        
        self.default_participant.fcm_tokens.update(unregistered=now)
        self.assertEqual(len(heartbeat_query()), 0)
        # and test many unregistered tokens...
        self.generate_fcm_token(self.default_participant, now)
        self.assertEqual(len(heartbeat_query()), 0)
        self.generate_fcm_token(self.default_participant, now)
        self.assertEqual(len(heartbeat_query()), 0)
        self.generate_fcm_token(self.default_participant, now)
        self.assertEqual(len(heartbeat_query()), 0)
        self.assertEqual(ParticipantFCMHistory.objects.count(), 4)
        # and then try setting a few to be registered again
        self.default_participant.fcm_tokens.first().update(unregistered=None)
        self.assertEqual(len(heartbeat_query()), 1)
        self.default_participant.fcm_tokens.first().update(unregistered=now)  # disable it
        self.assertEqual(len(heartbeat_query()), 0)
        self.default_participant.fcm_tokens.last().update(unregistered=None)  # a different one...
        self.assertEqual(len(heartbeat_query()), 1)
    
    def test_query_structure_no_fcm_token(self):
        self.set_working_heartbeat_notification_fully_valid
        self.assertListEqual(list(heartbeat_query()), self.default_participant_response)
    
    def test_query_structure_many_fcm_tokens_on_one_participant(self):
        # this isn't a valid state, it SHOULD be impossible, but we've never had an issue on the normal
        # push notifications
        self.set_working_heartbeat_notification_fully_valid
        self.generate_fcm_token(self.default_participant, None)
        self.generate_fcm_token(self.default_participant, None)
        self.generate_fcm_token(self.default_participant, None)
        correct = [
            (self.default_participant.id, fcm_token.token, ANDROID_API, DEFAULT_HEARTBEAT_MESSAGE)
            for fcm_token in self.default_participant.fcm_tokens.all()
        ]
        thing_to_test = list(heartbeat_query())
        # have to sort by the token value, order is intentionally randomized.
        correct.sort(key=lambda x: x[1])
        thing_to_test.sort(key=lambda x: x[1])
        self.assertListEqual(thing_to_test, correct)
    
    def test_multiple_valid_fcm_tokens2(self):
        # aaaaaaand I wrote this test before I saw the previous one. 🫠
        # This behavior is correct. For some reason a participant has multiple valid fcm tokens we
        # need to send a heartbeat notification to each of them.
        self.set_working_heartbeat_notification_fully_valid
        self.generate_fcm_token(self.default_participant, None)  # unregistered=None
        query = heartbeat_query()
        self.assertEqual(len(query), 2)
        # the output is INTENTIONALLY RANDOMLY SORTED, even though that is a lil stupid, so we need
        # to check the output in a way that doesn't care about order.
        self.assertIn(self.default_participant_response[0], query)
        the_other_one = (
            self.default_participant.id, self.default_participant.fcm_tokens.last().token,
            ANDROID_API, DEFAULT_HEARTBEAT_MESSAGE
        )
        self.assertIn(the_other_one, query)
        self.assertNotEqual(self.default_participant_response[0], the_other_one)
    
    def test_query_multiple_participants_with_only_one_valid(self):
        self.set_working_heartbeat_notification_fully_valid
        self.generate_participant(self.default_study)
        self.generate_participant(self.default_study)
        self.generate_participant(self.default_study)
        self.assertEqual(Participant.objects.all().count(), 4)
        self.assertEqual(len(heartbeat_query()), 1)
        self.assertListEqual(list(heartbeat_query()), self.default_participant_response)
    
    def test_query_multiple_participants_with_both_valid(self):
        self.set_working_heartbeat_notification_fully_valid
        p2 = self.generate_participant(self.default_study)
        self.generate_fcm_token(p2, None)
        p2.update(
            deleted=False, permanently_retired=False, last_upload=timezone.now() - timedelta(minutes=61),
        )
        self.assertEqual(Participant.objects.all().count(), 2)
        self.assertEqual(len(heartbeat_query()), 2)
        correct = self.default_participant_response
        correct.append((p2.id, p2.fcm_tokens.first().token, ANDROID_API, DEFAULT_HEARTBEAT_MESSAGE))
        thing_to_test = list(heartbeat_query())
        # have to sort by the token value, order is intentionally randomized.
        correct.sort(key=lambda x: x[1])
        thing_to_test.sort(key=lambda x: x[1])
        self.assertListEqual(thing_to_test, correct)
    
    @patch("services.celery_push_notifications.celery_heartbeat_send_push_notification")
    @patch("services.celery_push_notifications.check_firebase_instance")
    def test_heartbeat_notification_no_participants(
        self, check_firebase_instance: MagicMock, celery_heartbeat_send_push_notification: MagicMock,
    ):
        check_firebase_instance.return_value = True
        create_heartbeat_tasks()
        check_firebase_instance.assert_called_once()  # don't create heartbeat tasks without firebase
        celery_heartbeat_send_push_notification.assert_not_called()
        self.default_participant.refresh_from_db()
        self.assertIsNone(self.default_participant.last_heartbeat_notification)
    
    @patch("libs.push_notification_helpers.send_notification")
    @patch("services.celery_push_notifications.check_firebase_instance")
    def test_heartbeat_notification_one_participant(
        self, check_firebase_instance: MagicMock, send_notification: MagicMock,
    ):
        check_firebase_instance.return_value = True
        self.set_working_heartbeat_notification_fully_valid
        create_heartbeat_tasks()
        send_notification.assert_called_once()
        check_firebase_instance.assert_called()
        self.assertEqual(check_firebase_instance._mock_call_count, 2)
        self.default_participant.refresh_from_db()
        self.assertIsNotNone(self.default_participant.last_heartbeat_notification)
        self.assertIsInstance(self.default_participant.last_heartbeat_notification, datetime)
    
    @patch("libs.push_notification_helpers.send_notification")
    @patch("services.celery_push_notifications.check_firebase_instance")
    def test_heartbeat_notification_two_participants(
        self, check_firebase_instance: MagicMock, send_notification: MagicMock,
    ):
        check_firebase_instance.return_value = True
        self.set_working_heartbeat_notification_fully_valid
        p2 = self.generate_participant(self.default_study)
        self.generate_fcm_token(p2, None)
        p2.update(
            deleted=False, permanently_retired=False, last_upload=timezone.now() - timedelta(minutes=61),
        )
        
        create_heartbeat_tasks()
        send_notification.assert_called()   # each called twice
        check_firebase_instance.assert_called()
        self.default_participant.refresh_from_db()
        p2.refresh_from_db()
        self.assertIsNotNone(self.default_participant.last_heartbeat_notification)
        self.assertIsInstance(self.default_participant.last_heartbeat_notification, datetime)
        self.assertIsNotNone(p2.last_heartbeat_notification)
        self.assertIsInstance(p2.last_heartbeat_notification, datetime)
    
    @patch("libs.push_notification_helpers.send_notification")
    @patch("services.celery_push_notifications.check_firebase_instance")
    def test_heartbeat_notification_two_participants_one_failure(
        self, check_firebase_instance: MagicMock, send_notification: MagicMock,
    ):
        check_firebase_instance.return_value = True
        p2 = self.generate_participant(self.default_study)
        self.generate_fcm_token(p2, None)
        p2.update(
            deleted=False,
            permanently_retired=False,
            last_upload=timezone.now() - timedelta(minutes=61),
        )
        
        create_heartbeat_tasks()
        send_notification.assert_called()  # each called twice
        check_firebase_instance.assert_called()
        self.default_participant.refresh_from_db()
        p2.refresh_from_db()
        self.assertIsNone(self.default_participant.last_heartbeat_notification)
        self.assertIsNotNone(p2.last_heartbeat_notification)
        self.assertIsInstance(p2.last_heartbeat_notification, datetime)
    
    @patch("libs.push_notification_helpers.send_custom_notification_raw")
    @patch("services.celery_push_notifications.check_firebase_instance")
    def test_heartbeat_notification_errors(
        self, check_firebase_instance: MagicMock, send_custom_notification_raw: MagicMock,
    ):
        check_firebase_instance.return_value = True
        self.set_working_heartbeat_notification_fully_valid
        
        send_custom_notification_raw.side_effect = ValueError("test")
        self.assertRaises(ValueError, create_heartbeat_tasks)
        self.default_participant.refresh_from_db()
        self.assertIsNone(self.default_participant.last_heartbeat_notification)
        self.assertIsNone(self.default_participant.fcm_tokens.first().unregistered)
        
        send_custom_notification_raw.side_effect = ThirdPartyAuthError("test")
        self.assertRaises(ThirdPartyAuthError, create_heartbeat_tasks)
        self.default_participant.refresh_from_db()
        self.assertIsNone(self.default_participant.last_heartbeat_notification)
        self.assertIsNone(self.default_participant.fcm_tokens.first().unregistered)
    
    @patch("libs.push_notification_helpers.send_custom_notification_raw")
    @patch("services.celery_push_notifications.check_firebase_instance")
    def test_heartbeat_notification_errors_swallowed(
        self, check_firebase_instance: MagicMock, send_custom_notification_raw: MagicMock,
    ):
        check_firebase_instance.return_value = True
        self.set_working_heartbeat_notification_fully_valid
        
        # but these don't actually raise the error
        send_custom_notification_raw.side_effect = ThirdPartyAuthError("Auth error from APNS or Web Push Service")
        create_heartbeat_tasks()  # no error
        self.default_participant.refresh_from_db()
        self.assertIsNone(self.default_participant.last_heartbeat_notification)
        # issues a new query every time, don't need te refresh
        self.assertIsNone(self.default_participant.fcm_tokens.first().unregistered)
        
        send_custom_notification_raw.side_effect = SenderIdMismatchError("test")
        create_heartbeat_tasks()
        self.default_participant.refresh_from_db()
        self.assertIsNone(self.default_participant.last_heartbeat_notification)
        self.assertIsNone(self.default_participant.fcm_tokens.first().unregistered)
        
        send_custom_notification_raw.side_effect = SenderIdMismatchError("test")
        create_heartbeat_tasks()
        self.default_participant.refresh_from_db()
        self.assertIsNone(self.default_participant.last_heartbeat_notification)
        self.assertIsNone(self.default_participant.fcm_tokens.first().unregistered)
        
        send_custom_notification_raw.side_effect = QuotaExceededError("test")
        create_heartbeat_tasks()
        self.default_participant.refresh_from_db()
        self.assertIsNone(self.default_participant.last_heartbeat_notification)
        self.assertIsNone(self.default_participant.fcm_tokens.first().unregistered)
        
        send_custom_notification_raw.side_effect = ValueError("The default Firebase app does not exist")
        create_heartbeat_tasks()
        self.default_participant.refresh_from_db()
        self.assertIsNone(self.default_participant.last_heartbeat_notification)
        self.assertIsNone(self.default_participant.fcm_tokens.first().unregistered)
        
        # unregistered has the side effect of disabling the fcm token, so test it last
        send_custom_notification_raw.side_effect = UnregisteredError("test")
        create_heartbeat_tasks()
        self.default_participant.refresh_from_db()
        self.assertIsNone(self.default_participant.last_heartbeat_notification)
        self.assertIsInstance(self.default_participant.fcm_tokens.first().unregistered, datetime)


class TestResendLogicQuery(CommonTestCase):
    """ This test runs the missing_notification_checkin_query across a variety of scenarios, similar
    to the way TestHeartbeatQuery works. """
    
    # the default study is in America/New_York, absolute time events get _reconstructed_ to be in
    # this timezone.  Also we need times with only minute precision.
    NOW_SORTA = timezone.now().replace(second=0, microsecond=0).astimezone(gettz('America/New_York'))
    THE_BEGINNING_OF_TIME = NOW_SORTA - timedelta(days=1000)
    THE_PAST = NOW_SORTA - timedelta(days=5)
    THE_FUTURE = NOW_SORTA + timedelta(days=5)
    # get populated if you use the run_and_refresh_any_models helper
    BEFORE_RUN = None
    AFTER_RUN = None
    
    APP_VERSION = IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION
    
    def setUp(self):
        super().setUp()
        self.START_OF_TEST_TIME = timezone.now()
        self.default_absolute_schedule = self.generate_absolute_schedule_from_datetime(self.default_survey, self.THE_PAST)
        self.already_set_up_default_participant = False
        global_settings = GlobalSettings.get_singleton_instance()
        global_settings.update(earliest_possible_time_of_push_notification_resend=self.THE_BEGINNING_OF_TIME)
    
    def run_resend_logic_and_refresh_these_models(self, *args: UtilityModel):
        self.BEFORE_RUN = timezone.now()
        undelete_events_based_on_lost_notification_checkin()
        self.AFTER_RUN = timezone.now()
        for model in args:
            model.refresh_from_db()
    
    @property
    def setup_participant_resend_push_basics(self):
        if self.already_set_up_default_participant:
            return
        self.set_default_participant_all_push_notification_features
        # we are not testing fcm token details in these tests.
        self.default_participant.update(last_upload=self.NOW_SORTA)  # needs to be recently active
        self.already_set_up_default_participant = True
    
    @property
    def setup_participant_2(self) -> Participant:
        p2 = self.generate_participant(self.default_study)
        p2.update(last_version_name=self.APP_VERSION, os_type=IOS_API)
        ParticipantFCMHistory(token=self.DEFAULT_FCM_TOKEN + "x", participant=p2).save()
        return p2
    
    def do_setup_for_resend_with_no_notification_report(
        self, participant: Participant = None
    ) -> Tuple[ScheduledEvent, ArchivedEvent]:
        # single participant setups use the default participant
        if participant is None:
            participant = self.default_participant
            self.setup_participant_resend_push_basics
        
        # these aro stupid and obscure so I made some named functions to separate them
        sched_event = self._build_base_sched_event(participant)
        archive = self._build_base_archived_event(sched_event)
        self._attach_archive_to_scheduled_event_as_if_sent(sched_event, archive)
        return sched_event, archive
    
    def do_setup_for_resend_with_unapplied_notification_report(self) -> Tuple[ScheduledEvent, ArchivedEvent, SurveyNotificationReport]:
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        report = SurveyNotificationReport.objects.create(
            participant=self.default_participant,
            notification_uuid=archive.uuid,  # they are guaranteed identical with scheduled event
            applied=False
        )
        return sched_event, archive, report
    
    def _build_base_sched_event(self, participant: Participant) -> ScheduledEvent:
        # get our scheduled event and archived event, check uuids and scheduled time match
        sched_event = self.generate_scheduled_event(
            self.default_survey, participant, self.default_absolute_schedule, self.THE_PAST,
        )
        self.assertIsNotNone(sched_event.uuid)
        return sched_event
    
    def _build_base_archived_event(self, sched_event: ScheduledEvent) -> ArchivedEvent:
        archive = self.generate_archived_event_for_absolute_schedule(sched_event.absolute_schedule, sched_event.uuid)
        self.assertIsNotNone(archive.uuid)
        self.assertEqual(sched_event.uuid, archive.uuid)
        return archive
    
    def _attach_archive_to_scheduled_event_as_if_sent(self, sched_event: ScheduledEvent, archive: ArchivedEvent):
        # this value on archive is "reconstructed" to be in the study timezone, this check catches
        # if we change anything in our database test helpers like a timezone change.
        self.assertEquals(sched_event.scheduled_time, archive.scheduled_time)
        
        self.assertIsNone(sched_event.most_recent_event)
        self.assertFalse(sched_event.deleted)
        sched_event.most_recent_event = archive
        sched_event.deleted = True
        sched_event.save()
        self.assertGreater(sched_event.last_updated, self.NOW_SORTA)  # this is probably desireable
        
        # need some nontrivial setup on the archived event, force last updated to be THE_PAST
        # (we don't need to fiddle with the created on field, only last updated)
        ArchivedEvent.objects.filter(pk=archive.pk).update(last_updated=self.THE_PAST)
        archive.refresh_from_db()
        self.assertEqual(archive.last_updated, self.THE_PAST)
        self.assertEqual(archive.confirmed_received, False)
    
    # howto:
    # def do_setup_for_a_fully_applied_resend_loop(self) -> Tuple[ScheduledEvent, ArchivedEvent, SurveyNotificationReport]:
    # To do this you should literally run the test for that setup, it has return valueds:
    #    sched_event, archive, report = self.test_notification_report_correctly_blocks_resend()
    
    ## one-participant tests
    
    # null tests
    
    def test_no_data(self):
        self.assert_counts(0, 0, 0)
        undelete_events_based_on_lost_notification_checkin()
        self.assert_counts(0, 0, 0)
    
    def test_one_participant_nothing_else(self):
        self.using_default_participant()
        self.assert_counts(0, 0, 0)
        undelete_events_based_on_lost_notification_checkin()
        self.assert_counts(0, 0, 0)
    
    # version restrictions
    
    def test_ios_version_restriction_blocks(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_participant.update(os_type=IOS_API, last_version_name="2024.21")
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
    
    def test_ios_version_restriction_allows_equal(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_participant.update(os_type=IOS_API, last_version_name=self.APP_VERSION)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
    
    def test_ios_version_restriction_allows_higher(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_participant.update(os_type=IOS_API, last_version_name="2024.29")
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
    
    def test_android_os_restriction_blocks(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_participant.update(os_type=ANDROID_API)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
    
    # participant state restrictions
    
    def test_participant_inactive_more_than_one_week(self):
        # should not update anything in the database, we can check archive and schedule last updated
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_participant.update(last_upload=self.THE_PAST - timedelta(days=8))
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        # this will find and operate on no database objects, confirm with last_updated on archive
        # because that is supposed to change if it is identified and used
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertEqual(archive.last_updated, old_archive_last_updated)
        self.assertEqual(sched_event.last_updated, old_sched_event_last_updated)
        self.assert_last_updated_not_equal(archive, sched_event)
    
    def test_participant_deleted(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_participant.update(deleted=True)
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertEqual(archive.last_updated, old_archive_last_updated)
        self.assertEqual(sched_event.last_updated, old_sched_event_last_updated)
        self.assert_last_updated_not_equal(archive, sched_event)
    
    def test_participant_retired(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_participant.update(permanently_retired=True)
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertEqual(archive.last_updated, old_archive_last_updated)
        self.assertEqual(sched_event.last_updated, old_sched_event_last_updated)
        self.assert_last_updated_not_equal(archive, sched_event)
    
    # archivedevent and scheduledevent behavior
    
    # FIXME: this test failing is a bug, maybe a timezone bug, maybe something else
    from dateutil.tz import gettz
    t = datetime(2024, 11, 25, 18, 30, 0, 0, tzinfo=gettz("America/New_York"))
    # with time_machine.travel(t):
    @time_machine.travel(t)
    def test_uhoh_bug_archive_last_updated_less_than_30_minutes_ago_does_nothing(self):
        # recently updated archive should not result in resend
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        ArchivedEvent.fltr(pk=archive.pk).update(last_updated=self.NOW_SORTA - timedelta(minutes=29))
        old_archive_last_updated = archive.last_updated
        self.assertEqual(old_archive_last_updated, self.THE_PAST)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertNotEqual(archive.last_updated, old_archive_last_updated)
        self.assert_not_touched_in_last_run(archive)
        self.assert_not_touched_in_last_run(sched_event)
        self.assert_last_updated_not_equal(archive, sched_event)
    
    
    def test_archive_last_updated_less_than_30_minutes_ago_does_nothing(self):
        # recently updated archive should not result in resend
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        ArchivedEvent.fltr(pk=archive.pk).update(last_updated=self.NOW_SORTA - timedelta(minutes=29))
        old_archive_last_updated = archive.last_updated
        self.assertEqual(old_archive_last_updated, self.THE_PAST)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertNotEqual(archive.last_updated, old_archive_last_updated)
        self.assert_not_touched_in_last_run(archive)
        self.assert_not_touched_in_last_run(sched_event)
        self.assert_last_updated_not_equal(archive, sched_event)
    
    def test_null_uuid_on_archive(self):
        # should fail to show up in the query resulting in nothing
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        ArchivedEvent.objects.filter(pk=archive.pk).update(uuid=None)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertEqual(archive.last_updated, old_archive_last_updated)  # overkill...
        self.assertEqual(sched_event.last_updated, old_sched_event_last_updated)
        self.assert_not_touched_in_last_run(archive)
        self.assert_not_touched_in_last_run(sched_event)
        self.assert_last_updated_not_equal(archive, sched_event)
    
    def test_different_uuids_on_scheduled_event_and_archive(self):
        # last_updated should change on archive, uuid on archive should be set to None, scheduled
        # event should not be sendable, scheduled event should not be touched.
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        initial_uuid = sched_event.uuid
        old_archive_last_updated = archive.last_updated
        ArchivedEvent.objects.filter(pk=archive.pk).update(uuid=uuid.uuid4())
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertNotEqual(archive.last_updated, old_archive_last_updated)
        self.assert_touched_in_last_run(archive)
        self.assertIsNone(archive.uuid)
        self.assertEqual(sched_event.uuid, initial_uuid)
        self.assert_not_touched_in_last_run(sched_event)
        self.assert_last_updated_not_equal(archive, sched_event)
    
    def test_archive_with_uuid_and_scheduled_with_no_uuid(self):
        # last_updated should be updated, uuid on archive should be get set to None, scheduled event
        # should not be sendable, scheduled event uuid should still be None.
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        old_archive_last_updated = archive.last_updated
        ScheduledEvent.objects.filter(pk=sched_event.pk).update(uuid=None)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertNotEqual(archive.last_updated, old_archive_last_updated)
        self.assert_touched_in_last_run(archive)
        self.assertIsNone(archive.uuid)
        self.assertIsNone(sched_event.uuid)
        self.assert_not_touched_in_last_run(sched_event)
        self.assert_last_updated_not_equal(archive, sched_event)
    
    def test_archive_with_no_uuid_and_scheduled_with_uuid(self):
        # nothing should change
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        ArchivedEvent.objects.filter(pk=archive.pk).update(uuid=None)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertEqual(archive.last_updated, old_archive_last_updated)  # overkill
        self.assertEqual(sched_event.last_updated, old_sched_event_last_updated)
        self.assert_not_touched_in_last_run(sched_event)
        self.assert_not_touched_in_last_run(archive)
        self.assert_last_updated_not_equal(archive, sched_event)
    
    def test_archive_with_no_uuid_and_scheduled_with_no_uuid(self):
        # identical to test_archive_with_no_uuid_and_scheduled_with_uuid but we clear both uuids)
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        ArchivedEvent.objects.filter(pk=archive.pk).update(uuid=None)
        ScheduledEvent.objects.filter(pk=sched_event.pk).update(uuid=None)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertEqual(archive.last_updated, old_archive_last_updated)
        self.assertEqual(sched_event.last_updated, old_sched_event_last_updated)
        self.assert_not_touched_in_last_run(sched_event)
        self.assert_not_touched_in_last_run(archive)
        self.assert_last_updated_not_equal(archive, sched_event)
    
    def test_archive_with_uuid_and_scheduled_actually_deleted(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        # database should have 3 of our 4 relevant tables with 1, SurveyNotificationReport should have 0
        self.assertEqual(archive.last_updated, self.THE_PAST)
        self.assert_counts(1, 1, 0)
        ScheduledEvent.objects.all().delete()
        self.assert_counts(0, 1, 0)
        self.run_resend_logic_and_refresh_these_models(archive)  # sched_event doesn't exist, derp
        self.assert_counts(0, 1, 0)
        archive.refresh_from_db()
        self.assertGreater(archive.last_updated, self.BEFORE_RUN)
        self.assertLess(archive.last_updated, self.AFTER_RUN)
        self.assertIsNone(archive.uuid)
    
    def test_not_resendable_with_scheduled_event_that_has_no_schedule(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        sched_event.update(absolute_schedule=None)
        AbsoluteSchedule.objects.all().delete()  # its a absolute
        RelativeSchedule.objects.all().delete()
        WeeklySchedule.objects.all().delete()
        sched_event.refresh_from_db()  # errors if the cascade still applied
        # this is technically invalid database state, but the error occurs over in the push
        # notification becausethe schedule's type cannot be determined, and that causes an infinite
        # resend loop, which is REAL BAD.
        self.assertEqual(archive.last_updated, self.THE_PAST)
        self.assert_counts(1, 1, 0)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_counts(1, 1, 0)
        # self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertGreater(archive.last_updated, self.BEFORE_RUN)
        self.assertLess(archive.last_updated, self.AFTER_RUN)
        self.assertGreater(sched_event.last_updated, self.BEFORE_RUN)
        self.assertLess(sched_event.last_updated, self.AFTER_RUN)
        self.assertIsNone(archive.uuid)
        self.assertIsNone(sched_event.uuid)
    
    
    def test_schedule_already_enabled(self):
        # should "work" in the sense that all the database objects will be modified and correct.
        # but I don't thin this state should ever happen.
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        old_archive_last_updated = archive.last_updated
        ScheduledEvent.objects.filter(pk=sched_event.pk).update(deleted=False)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
        self.assert_last_updated_equal(archive, sched_event)
    
    def test_notification_report_correctly_blocks_a_followup_resend(self):
        sched_event, archive, report = self.do_setup_for_resend_with_unapplied_notification_report()
        self.assert_scheduled_event_not_sendable(sched_event)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive, report)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_not_touched_in_last_run(sched_event)
        self.assert_touched_in_last_run(archive)
        self.assert_touched_in_last_run(report)
        self.assert_last_updated_equal(archive, report)
        self.assert_last_updated_not_equal(archive, sched_event)
        self.assertTrue(report.applied)
        self.assertTrue(archive.confirmed_received)
        return sched_event, archive, report
    
    def test_doing_it_twice_in_succession_still_blocks(self):
        sched_event, archive, report = self.do_setup_for_resend_with_unapplied_notification_report()
        self.assert_scheduled_event_not_sendable(sched_event)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive, report)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_touched_in_last_run(archive)
        self.assert_not_touched_in_last_run(sched_event)
        self.assert_touched_in_last_run(report)
        self.assert_last_updated_equal(archive, report)
        self.assertTrue(report.applied)
        self.assertTrue(archive.confirmed_received)
        # now we do it again
        old_before_run = self.BEFORE_RUN
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        self.run_resend_logic_and_refresh_these_models(sched_event, archive, report)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertGreater(self.BEFORE_RUN, old_before_run)  # sanity check
        self.assertEqual(archive.last_updated, old_archive_last_updated)
        self.assertEqual(sched_event.last_updated, old_sched_event_last_updated)
        self.assert_not_touched_in_last_run(sched_event)
        self.assert_not_touched_in_last_run(archive)
        self.assert_not_touched_in_last_run(report)
        self.assertTrue(report.applied)
        self.assertTrue(archive.confirmed_received)
    
    # WORKING SCENARIOS
    
    def test_participant_with_minimum_requirements_resends(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        # database should have 3 of our 4 relevant tables with 1, SurveyNotificationReport should have 0
        self.assertEqual(archive.last_updated, self.THE_PAST)
        self.assert_counts(1, 1, 0)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_counts(1, 1, 0)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
    
    def test_participant_inactive_less_than_one_week(self):
        # should be duplicate test of minimum requirements
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_participant.update(last_upload=self.THE_PAST + timedelta(days=6))  # yesterday
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
    
    def test_archive_last_updated_more_than_30_minutes_ago(self):
        # should be duplicate test of minimum requirements
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        ArchivedEvent.objects.filter(pk=archive.pk).update(
            last_updated=self.NOW_SORTA - timedelta(minutes=32))
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
        # probably overkill or redundant
        self.assertGreater(archive.last_updated, old_archive_last_updated)
        self.assertGreater(sched_event.last_updated, old_sched_event_last_updated)
    
    def test_removed_scheduled_event_works_as_desired(self):
        # the archive and report should be updated, no uuid on archive left in place
        sched_event, archive, report = self.do_setup_for_resend_with_unapplied_notification_report()
        sched_event.delete()
        del sched_event
        self.run_resend_logic_and_refresh_these_models(archive, report)
        self.assert_touched_in_last_run(archive)
        self.assert_touched_in_last_run(report)
        self.assertTrue(report.applied)
        self.assertTrue(archive.confirmed_received)
        self.assertEqual(archive.uuid, report.notification_uuid)
        self.assert_last_updated_equal(archive, report)
        self.assertFalse(ScheduledEvent.objects.exists())
    
    ## 2 participant tests
    
    def test_2_participants_both_resend(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        p2 = self.setup_participant_2
        p2_sched_event, p2_archive = self.do_setup_for_resend_with_no_notification_report(p2)
        # database should have 3 of our 4 relevant tables with 1, SurveyNotificationReport should have 0
        self.assertEqual(archive.last_updated, self.THE_PAST)
        self.assertEqual(p2_archive.last_updated, self.THE_PAST)
        self.assert_counts(2, 2, 0)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive, p2_sched_event, p2_archive)
        self.assert_counts(2, 2, 0)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
        self.assert_resend_logic_reenabled_schedule_correctly(p2_sched_event, p2_archive)
    
    def test_2_participants_one_has_as_unapplied_report_one_has_no_report(self):
        sched_event, archive, report = self.do_setup_for_resend_with_unapplied_notification_report()
        p2 = self.setup_participant_2
        p2_sched_event, p2_archive = self.do_setup_for_resend_with_no_notification_report(p2)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive, report, p2_sched_event, p2_archive)
        # p1
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_touched_in_last_run(archive)
        self.assert_touched_in_last_run(report)
        self.assert_not_touched_in_last_run(sched_event)
        # p2
        self.assert_resend_logic_reenabled_schedule_correctly(p2_sched_event, p2_archive)
        # everything across participants should have the same last_updated time (except sched_event)
        self.assert_last_updated_equal(p2_archive, p2_sched_event)
        self.assert_last_updated_equal(archive, report)
        self.assert_last_updated_equal(archive, p2_archive)
        self.assert_last_updated_not_equal(archive, sched_event)
    
    def test_2_participants_one_has_resent_and_confirmed_one_has_to_resend(self):
        # the best way to get this state is to run the test for it.
        sched_event, archive, report = self.test_notification_report_correctly_blocks_a_followup_resend()
        # set up the second participant
        p2 = self.setup_participant_2
        p2_sched_event, p2_archive = self.do_setup_for_resend_with_no_notification_report(p2)
        self.run_resend_logic_and_refresh_these_models(
            sched_event, archive, report, p2_sched_event, p2_archive
        )
        # p1
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_not_touched_in_last_run(archive)
        self.assert_not_touched_in_last_run(report)
        self.assert_not_touched_in_last_run(sched_event)
        self.assert_last_updated_equal(archive, report)
        # p2
        self.assert_resend_logic_reenabled_schedule_correctly(p2_sched_event, p2_archive)
        self.assert_touched_in_last_run(p2_archive)
        self.assert_touched_in_last_run(p2_sched_event)
        self.assert_last_updated_equal(p2_archive, p2_sched_event)
    
    # multiple archive and scheduled events
    
    def test_multiple_archive_and_scheduled_events_on_one_survey(self):
        # technically this creates bade database state of multiple scheduled events associated with
        # one absolute schedule
        sched_event_1, archive_1 = self.do_setup_for_resend_with_no_notification_report()
        sched_event_2, archive_2 = self.do_setup_for_resend_with_no_notification_report()
        self.assertNotEqual(archive_1.uuid, archive_2.uuid)
        self.assertNotEqual(sched_event_1.uuid, sched_event_2.uuid)
        self.run_resend_logic_and_refresh_these_models(sched_event_1, archive_1, sched_event_2, archive_2)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event_1, archive_1)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event_2, archive_2)
        self.assert_last_updated_equal(sched_event_1, sched_event_2)
        self.assert_last_updated_equal(archive_1, archive_2)
    
    def test_multiple_archive_and_scheduled_events_on_two_surveys(self):
        sched_event_1, archive_1 = self.do_setup_for_resend_with_no_notification_report()
        another_survey = self.generate_survey(self.default_study, Survey.TRACKING_SURVEY)
        self.default_absolute_schedule = self.generate_absolute_schedule_from_datetime(another_survey, self.THE_PAST)
        sched_event_2, archive_2 = self.do_setup_for_resend_with_no_notification_report()
        self.assertNotEqual(archive_1.uuid, archive_2.uuid)
        self.assertNotEqual(sched_event_1.uuid, sched_event_2.uuid)
        self.run_resend_logic_and_refresh_these_models(sched_event_1, archive_1, sched_event_2, archive_2)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event_1, archive_1)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event_2, archive_2)
        self.assert_last_updated_equal(sched_event_1, sched_event_2)
        self.assert_last_updated_equal(archive_1, archive_2)
    
    ## don't run before earliest resend time
    
    def test_correct_setup_before_earliest_resend_time_restriction_fails(self):
        global_settings = GlobalSettings.get_singleton_instance()
        global_settings.update(earliest_possible_time_of_push_notification_resend=self.THE_FUTURE)
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.assert_scheduled_event_not_sendable(sched_event)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_not_touched_in_last_run(archive)
        self.assert_not_touched_in_last_run(sched_event)
    
    def test_resend_actually_creates_db_state_where_push_notifications_are_sent(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        
        # no notifications should be sent
        surveys_1, schedules_1, patient_ids_1 = get_surveys_and_schedules(timezone.now())
        self.assertEqual(surveys_1, {})
        self.assertEqual(schedules_1, {})
        self.assertEqual(patient_ids_1, {})
        
        # do the thing
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
        
        # exactly the one notification should be sent
        fcm_token = self.default_participant.fcm_tokens.get().token
        surveys_2, schedules_2, patient_ids_2 = get_surveys_and_schedules(timezone.now())
        self.assertEqual(surveys_2, {fcm_token: [self.default_survey.object_id]})
        self.assertEqual(schedules_2, {fcm_token: [sched_event.id]})
        self.assertEqual(patient_ids_2, {fcm_token: self.default_participant.patient_id})
    
    ## tools
    
    def assert_resend_logic_reenabled_schedule_correctly(self, sched_event: ScheduledEvent, archive: ArchivedEvent):
        ## this is the core test that resend logic found and reset a scheduled event.
        # The archive has been "touched", and confirmed_received is False
        self.assertNotEqual(self.THE_PAST, archive.last_updated)
        self.assert_touched_in_last_run(archive)
        self.assertEqual(archive.confirmed_received, False)
        
        # the scheduled event has deleted=False, was updated during logic
        self.assertEqual(sched_event.deleted, False)
        self.assert_touched_in_last_run(sched_event)
        
        # all Notification reports should be applied (there may be None)
        for notification_report in SurveyNotificationReport.objects.all():
            self.assertEqual(notification_report.applied, True)
        
        # the logic updates everything with the same last_updated time
        self.assert_last_updated_equal(archive, sched_event)
    
    def assert_scheduled_event_not_sendable(self, sched_event: ScheduledEvent):
        self.assertEqual(sched_event.deleted, True)
    
    def assert_touched_in_last_run(self, model: TimestampedModel):
        try:
            self.assertGreater(model.last_updated, self.BEFORE_RUN)
            self.assertLess(model.last_updated, self.AFTER_RUN)
        except AssertionError:
            model.pprint
            raise
    
    def assert_not_touched_in_last_run(self, model: TimestampedModel):
        self.assertLess(model.last_updated, self.BEFORE_RUN)
        self.assertLess(model.last_updated, self.AFTER_RUN)
    
    def assert_last_updated_equal(self, a: TimestampedModel, b: TimestampedModel):
        self.assertEqual(a.last_updated, b.last_updated)
    
    def assert_last_updated_not_equal(self, a: TimestampedModel, b: TimestampedModel):
        self.assertNotEqual(a.last_updated, b.last_updated)
    
    def assert_counts(self, scheduled_event: int, archived_event: int, notification_report: int):
        self.assertEqual(ScheduledEvent.objects.count(), scheduled_event)
        self.assertEqual(ArchivedEvent.objects.count(), archived_event)
        self.assertEqual(SurveyNotificationReport.objects.count(), notification_report)
