# trunk-ignore-all(ruff/B018,bandit/B101)
from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from django.utils import timezone
from firebase_admin.messaging import (QuotaExceededError, SenderIdMismatchError,
    ThirdPartyAuthError, UnregisteredError)

from constants.message_strings import DEFAULT_HEARTBEAT_MESSAGE
from constants.user_constants import ACTIVE_PARTICIPANT_FIELDS, ANDROID_API
from database.user_models_participant import Participant, ParticipantFCMHistory
from services.celery_push_notifications import create_heartbeat_tasks
from services.heartbeat_push_notifications import heartbeat_query
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
    
    @patch("services.heartbeat_push_notifications.celery_heartbeat_send_push_notification_task")
    @patch("services.celery_push_notifications.check_firebase_instance")
    def test_heartbeat_notification_no_participants(
        self, check_firebase_instance: MagicMock, celery_heartbeat_send_push_notification_task: MagicMock,
    ):
        check_firebase_instance.return_value = True
        create_heartbeat_tasks()
        check_firebase_instance.assert_called_once()  # don't create heartbeat tasks without firebase
        celery_heartbeat_send_push_notification_task.assert_not_called()
        self.default_participant.refresh_from_db()
        self.assertIsNone(self.default_participant.last_heartbeat_notification)
    
    @patch("libs.push_notification_helpers.send_notification")
    @patch("services.heartbeat_push_notifications.check_firebase_instance")
    @patch("services.celery_push_notifications.check_firebase_instance")
    def test_heartbeat_notification_one_participant(
        self, check_firebase_instance: MagicMock, check_firebase_instance2: MagicMock, send_notification: MagicMock,
    ):
        check_firebase_instance.return_value = True
        check_firebase_instance2.return_value = True
        self.set_working_heartbeat_notification_fully_valid
        create_heartbeat_tasks()
        send_notification.assert_called_once()
        check_firebase_instance.assert_called()
        check_firebase_instance2.assert_called()
        self.default_participant.refresh_from_db()
        self.assertIsNotNone(self.default_participant.last_heartbeat_notification)
        self.assertIsInstance(self.default_participant.last_heartbeat_notification, datetime)
    
    @patch("libs.push_notification_helpers.send_notification")
    @patch("services.heartbeat_push_notifications.check_firebase_instance")
    @patch("services.celery_push_notifications.check_firebase_instance")
    def test_heartbeat_notification_two_participants(
        self, check_firebase_instance: MagicMock, check_firebase_instance2: MagicMock, send_notification: MagicMock,
    ):
        check_firebase_instance.return_value = True
        check_firebase_instance2.return_value = True
        self.set_working_heartbeat_notification_fully_valid
        p2 = self.generate_participant(self.default_study)
        self.generate_fcm_token(p2, None)
        p2.update(
            deleted=False, permanently_retired=False, last_upload=timezone.now() - timedelta(minutes=61),
        )
        
        create_heartbeat_tasks()
        send_notification.assert_called()
        check_firebase_instance.assert_called()
        check_firebase_instance2.assert_called()
        self.default_participant.refresh_from_db()
        p2.refresh_from_db()
        self.assertIsNotNone(self.default_participant.last_heartbeat_notification)
        self.assertIsInstance(self.default_participant.last_heartbeat_notification, datetime)
        self.assertIsNotNone(p2.last_heartbeat_notification)
        self.assertIsInstance(p2.last_heartbeat_notification, datetime)
    
    @patch("libs.push_notification_helpers.send_notification")
    @patch("services.heartbeat_push_notifications.check_firebase_instance")
    @patch("services.celery_push_notifications.check_firebase_instance")
    def test_heartbeat_notification_two_participants_one_failure(
        self, check_firebase_instance: MagicMock, check_firebase_instance2: MagicMock, send_notification: MagicMock,
    ):
        check_firebase_instance.return_value = True
        check_firebase_instance2.return_value = True
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
        check_firebase_instance2.assert_called()
        self.default_participant.refresh_from_db()
        p2.refresh_from_db()
        self.assertIsNone(self.default_participant.last_heartbeat_notification)
        self.assertIsNotNone(p2.last_heartbeat_notification)
        self.assertIsInstance(p2.last_heartbeat_notification, datetime)
    
    @patch("libs.push_notification_helpers.send_custom_notification_raw")
    @patch("services.heartbeat_push_notifications.check_firebase_instance")
    @patch("services.celery_push_notifications.check_firebase_instance")
    def test_heartbeat_notification_errors(
        self, check_firebase_instance: MagicMock, check_firebase_instance2: MagicMock, send_custom_notification_raw: MagicMock,
    ):
        check_firebase_instance.return_value = True
        check_firebase_instance2.return_value = True
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
    @patch("services.heartbeat_push_notifications.check_firebase_instance")
    @patch("services.celery_push_notifications.check_firebase_instance")
    def test_heartbeat_notification_errors_swallowed(
        self, check_firebase_instance: MagicMock, check_firebase_instance2: MagicMock, send_custom_notification_raw: MagicMock,
    ):
        check_firebase_instance.return_value = True
        check_firebase_instance2.return_value = True
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
