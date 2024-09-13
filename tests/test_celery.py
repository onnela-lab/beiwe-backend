from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import time_machine
from dateutil.tz import gettz
from django.utils import timezone
from firebase_admin.messaging import (QuotaExceededError, SenderIdMismatchError,
    ThirdPartyAuthError, UnregisteredError)

from constants.message_strings import DEFAULT_HEARTBEAT_MESSAGE
from constants.testing_constants import (THURS_OCT_6_NOON_2022_NY, THURS_OCT_13_NOON_2022_NY,
    THURS_OCT_20_NOON_2022_NY, WEDNESDAY_JUNE_NOON_8_2022_EDT)
from constants.user_constants import ACTIVE_PARTICIPANT_FIELDS, ANDROID_API
from database.schedule_models import AbsoluteSchedule, ScheduledEvent
from database.user_models_participant import Participant, ParticipantFCMHistory
from services.celery_push_notifications import (create_heartbeat_tasks, get_surveys_and_schedules,
    heartbeat_query)
from tests.common import CommonTestCase


# trunk-ignore-all(ruff/B018)

class TestCelery(CommonTestCase):
    """ We don't actually have anything custom here (yet). """


class TestGetSurveysAndSchedules(TestCelery):
    
    def test_empty_db(self):
        self.assertEqual(ScheduledEvent.objects.count(), 0)
        self.validate_no_schedules()
    
    def validate_no_schedules(self):
        surveys, schedules, patient_ids = get_surveys_and_schedules(timezone.now())
        self.assertEqual(surveys, {})
        self.assertEqual(schedules, {})
        self.assertEqual(patient_ids, {})
    
    def validate_basics(self, schedule: ScheduledEvent):
        surveys, schedules, patient_ids = get_surveys_and_schedules(timezone.now())
        self.assertEqual(surveys, {self.DEFAULT_FCM_TOKEN: [self.DEFAULT_SURVEY_OBJECT_ID]})
        self.assertEqual(schedules, {self.DEFAULT_FCM_TOKEN: [schedule.pk]})
        self.assertEqual(patient_ids, {self.DEFAULT_FCM_TOKEN: self.DEFAULT_PARTICIPANT_NAME})
    
    # just a placeholder for future work, send_notification not actually called in this test
    @patch('services.celery_push_notifications.send_notification')
    def test_absolute_success(self, send_notification: MagicMock):
        send_notification.return_value = None
        
        self.populate_default_fcm_token
        the_past = timezone.now() + timedelta(days=-5)
        # an absolute survey 5 days in the past
        schedule = self.generate_easy_absolute_schedule_event_with_absolute_schedule(the_past)
        self.validate_basics(schedule)
    
    def test_absolute_fail(self):
        self.populate_default_fcm_token
        future = timezone.now() + timedelta(days=5)
        # an absolute survey 5 days in the future
        self.generate_easy_absolute_schedule_event_with_absolute_schedule(future)
        self.validate_no_schedules()
    
    def test_relative_success(self):
        self.populate_default_fcm_token
        # a relative survey 5 days in the past
        schedule = self.generate_easy_relative_schedule_event_with_relative_schedule(timedelta(days=-5))
        surveys, schedules, patient_ids = get_surveys_and_schedules(timezone.now())
        self.validate_basics(schedule)
    
    def test_relative_failure(self):
        self.populate_default_fcm_token
        # a relative survey 5 days in the past
        self.generate_easy_relative_schedule_event_with_relative_schedule(timedelta(days=5))
        self.validate_no_schedules()
    
    @time_machine.travel(THURS_OCT_6_NOON_2022_NY)
    def test_weekly_success(self):
        self.populate_default_fcm_token
        # a weekly survey, on a friday, sunday is the zero-index; I hate it more than you.
        schedule, count_created = self.generate_a_real_weekly_schedule_event_with_schedule(5)
        self.assertEqual(count_created, 1)
        with time_machine.travel(THURS_OCT_20_NOON_2022_NY):
            self.validate_basics(schedule)
    
    @time_machine.travel(THURS_OCT_6_NOON_2022_NY)
    def test_weekly_in_future_fails(self):
        self.populate_default_fcm_token
        # a weekly survey, on a friday, sunday is the zero-index; I hate it more than you.
        schedule, count_created = self.generate_a_real_weekly_schedule_event_with_schedule(5)
        self.assertEqual(count_created, 1)
        self.validate_no_schedules()
    
    @time_machine.travel(THURS_OCT_13_NOON_2022_NY)
    def test_time_zones(self):
        self.populate_default_fcm_token
        self.default_study.update_only(timezone_name='America/New_York')  # default in tests is normally UTC.
        
        # need to time travel to the past to get the weekly logic to produce the correct time
        with time_machine.travel(THURS_OCT_6_NOON_2022_NY):
            # creates a weekly survey for 2022-10-13 12:00:00-04:00
            schedule, count_created = self.generate_a_real_weekly_schedule_event_with_schedule(4, 12, 0)
            self.assertEqual(count_created, 1)
        
        # assert schedule time is equal to 2022-10-13 12:00:00-04:00, then assert components are equal.
        self.assertEqual(schedule.scheduled_time, THURS_OCT_13_NOON_2022_NY)
        self.assertEqual(schedule.scheduled_time.year, 2022)
        self.assertEqual(schedule.scheduled_time.month, 10)
        self.assertEqual(schedule.scheduled_time.day, 13)
        self.assertEqual(schedule.scheduled_time.hour, 12)
        self.assertEqual(schedule.scheduled_time.minute, 0)
        self.assertEqual(schedule.scheduled_time.second, 0)
        self.assertEqual(schedule.scheduled_time.tzinfo, gettz("America/New_York"))
        
        # set default participant to pacific time, assert that no push notification is calculated.
        self.default_participant.try_set_timezone('America/Los_Angeles')
        self.validate_no_schedules()
        
        # set the time zone to mountain time, assert that no push notification is calculated.
        self.default_participant.try_set_timezone('America/Denver')
        self.validate_no_schedules()
        
        # set the time zone to central time, assert that no push notification is calculated.
        self.default_participant.try_set_timezone('America/Chicago')
        self.validate_no_schedules()
        
        # but if you set the time zone to New_York the push notification is calculated!
        self.default_participant.try_set_timezone('America/New_York')
        self.validate_basics(schedule)
    
    @time_machine.travel(WEDNESDAY_JUNE_NOON_8_2022_EDT)
    def test_no_timezone_bug(self):
        plus_one_hour = WEDNESDAY_JUNE_NOON_8_2022_EDT + timedelta(hours=1)
        minus_one_hour = WEDNESDAY_JUNE_NOON_8_2022_EDT - timedelta(hours=1)
        minus_two_hours = WEDNESDAY_JUNE_NOON_8_2022_EDT - timedelta(hours=2)
        minus_three_hours = WEDNESDAY_JUNE_NOON_8_2022_EDT - timedelta(hours=3)
        minus_four_hours = WEDNESDAY_JUNE_NOON_8_2022_EDT - timedelta(hours=4)
        minus_five_hours = WEDNESDAY_JUNE_NOON_8_2022_EDT - timedelta(hours=5)
        # GMT_time = WEDNESDAY_JUNE_NOON_8_2022_EDT.replace(tzinfo=gettz('GMT'))
        self.populate_default_fcm_token
        
        # we have a bug where if the participant's timezone shifts it into the past it will not be
        # noticed by the survey schedule query until the study-timezone-based ScheduledEvent time is
        # in the past (has already passed).
        # Testing this with an absolute schedule.
        self.default_study.update_only(timezone_name='America/New_York')
        scheduled_event = self.generate_easy_absolute_schedule_event_with_absolute_schedule(WEDNESDAY_JUNE_NOON_8_2022_EDT)
        absolute_schedule = AbsoluteSchedule.objects.get()  # will fail if there is more than one, which would be a bug
        
        self.default_participant.try_set_timezone('GMT')
        self.default_participant.refresh_from_db()
        self.assertEqual(self.default_participant.timezone_name, 'GMT')
        
        # EDT is +4 hours of GMT, so the schedule "should trigger" at 12:00 GMT, but instead
        # triggers at 12:00 EDT, because the participant's timezone incorrectly checked.
        
        with time_machine.travel(plus_one_hour):
            self.validate_basics(scheduled_event)
        
        with time_machine.travel(minus_one_hour):
            self.validate_basics(scheduled_event)
        
        with time_machine.travel(minus_two_hours):
            self.validate_basics(scheduled_event)
        
        with time_machine.travel(minus_three_hours):
            self.validate_basics(scheduled_event)
        
        with time_machine.travel(minus_four_hours):
            self.validate_basics(scheduled_event)
        
        with time_machine.travel(minus_five_hours):
            self.validate_no_schedules()
    
    # using weekly as a base we now test situations where it shouldn't return schedules
    @time_machine.travel(THURS_OCT_6_NOON_2022_NY)
    def test_deleted_hidden_study(self):
        self.populate_default_fcm_token
        # a weekly survey, on a friday, sunday is the zero-index; I hate it more than you.
        schedule, count_created = self.generate_a_real_weekly_schedule_event_with_schedule(5)
        self.assertEqual(count_created, 1)
        self.default_study.update(deleted=True)
        with time_machine.travel(THURS_OCT_20_NOON_2022_NY):
            self.validate_no_schedules()
    
    @time_machine.travel(THURS_OCT_6_NOON_2022_NY)
    def test_manually_stopped_study(self):
        self.populate_default_fcm_token
        # a weekly survey, on a friday, sunday is the zero-index; I hate it more than you.
        schedule, count_created = self.generate_a_real_weekly_schedule_event_with_schedule(5)
        self.assertEqual(count_created, 1)
        self.default_study.update(manually_stopped=True)
        with time_machine.travel(THURS_OCT_20_NOON_2022_NY):
            self.validate_no_schedules()
    
    @time_machine.travel(THURS_OCT_6_NOON_2022_NY)
    def test_past_end_date(self):
        self.populate_default_fcm_token
        # a weekly survey, on a friday, sunday is the zero-index; I hate it more than you.
        schedule, count_created = self.generate_a_real_weekly_schedule_event_with_schedule(5)
        self.assertEqual(count_created, 1)
        # not testing time zones, just testing end date
        self.default_study.update(end_date=timezone.now().date() - timedelta(days=10))
        with time_machine.travel(THURS_OCT_20_NOON_2022_NY):
            self.validate_no_schedules()
    


class TestHeartbeatQuery(TestCelery):
    # this test class relies on behavior of the FalseCeleryApp class. Specifically, FalseCeleryApps
    # immediately run the created task synchronously, e.g. calls through safe_apply_async simply run
    # the target function on the same thread completely bypassing Celery.
    
    @property
    def set_working_heartbeat_notification_basics(self):
        # we are not testing fcm token details in these tests.
        self.default_participant.update(deleted=False, permanently_retired=False)
        self.populate_default_fcm_token
    
    @property
    def set_working_heartbeat_notification_fully_valid(self):
        # we will set last upload to as our active field, it can be any of the active fields.
        # after calling this function the default participant should be found by the heartbeat query
        now = timezone.now()
        self.default_participant.update(
            deleted=False, permanently_retired=False, last_upload=now - timedelta(minutes=61),
        )
        self.populate_default_fcm_token
    
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
        self.set_working_heartbeat_notification_basics
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
