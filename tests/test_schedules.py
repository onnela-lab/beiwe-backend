# trunk-ignore-all(bandit/B101,bandit/B106,ruff/B018,ruff/E701)
from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta
from typing import List
from unittest.mock import MagicMock, patch

import time_machine
from dateutil import tz
from dateutil.tz import gettz
from django.utils import timezone

from constants.schedule_constants import EMPTY_WEEKLY_SURVEY_TIMINGS
from constants.testing_constants import (EDT_WEEK, EST_WEEK, MIDNIGHT_EVERY_DAY_OF_WEEK,
    MONDAY_JUNE_NOON_6_2022_EDT, NOON_EVERY_DAY_OF_WEEK, THURS_OCT_6_NOON_2022_NY,
    THURS_OCT_13_NOON_2022_NY, THURS_OCT_20_NOON_2022_NY, WEDNESDAY_JUNE_NOON_8_2022_EDT)
from database.schedule_models import (AbsoluteSchedule, ArchivedEvent, BadWeeklyCount, Intervention,
    InterventionDate, RelativeSchedule, ScheduledEvent, WeeklySchedule)
from database.survey_models import Survey
from libs.schedules import (export_weekly_survey_timings, get_next_weekly_event_and_schedule,
    get_start_and_end_of_java_timings_week, NoSchedulesException,
    repopulate_absolute_survey_schedule_events, repopulate_all_survey_scheduled_events,
    repopulate_relative_survey_schedule_events, repopulate_weekly_survey_schedule_events)
from services.celery_push_notifications import get_surveys_and_schedules
from services.scripts_runner import push_notification_scheduledevent_rebuild
from tests.common import CommonTestCase


# timezones should be compared using the 'is' operator
THE_ONE_TRUE_TIMEZONE = gettz("America/New_York")
THE_OTHER_ACCEPTABLE_TIMEZONE = gettz("UTC")

SCHEDULEDEVENT_IDENTITY_FIELDS = [
    "participant_id",
    "survey_id",
    "scheduled_time",
    "weekly_schedule",
    "relative_schedule",
    "absolute_schedule",
    "pk",
    "uuid",
]

class BadScheduleState(Exception): pass


class TestGetSurveysAndSchedulesQuery(CommonTestCase):
    
    def assert_no_schedules(self):
        surveys, schedules, patient_ids = get_surveys_and_schedules(timezone.now())
        self.assertEqual(surveys, {})
        self.assertEqual(schedules, {})
        self.assertEqual(patient_ids, {})
    
    def assert_default_schedule_found(self, schedule: ScheduledEvent):
        surveys, schedules, patient_ids = get_surveys_and_schedules(timezone.now())
        self.assertEqual(surveys, {self.DEFAULT_FCM_TOKEN: [self.DEFAULT_SURVEY_OBJECT_ID]})
        self.assertEqual(schedules, {self.DEFAULT_FCM_TOKEN: [schedule.pk]})
        self.assertEqual(patient_ids, {self.DEFAULT_FCM_TOKEN: self.DEFAULT_PARTICIPANT_NAME})
    
    #
    ## Tests!
    #
    
    def test_empty_db(self):
        self.assertEqual(ScheduledEvent.objects.count(), 0)
        self.assert_no_schedules()
    
    def test_absolute_fail(self):
        self.default_fcm_token
        future = timezone.now() + timedelta(days=5)
        # an absolute survey 5 days in the future
        self.generate_easy_absolute_scheduled_event_with_absolute_schedule(future)
        self.assert_no_schedules()
    
    def test_relative_success(self):
        self.default_fcm_token
        # a relative survey 5 days in the past
        schedule = self.generate_easy_relative_schedule_event_with_relative_schedule(timedelta(days=-5))
        surveys, schedules, patient_ids = get_surveys_and_schedules(timezone.now())
        self.assert_default_schedule_found(schedule)
    
    def test_relative_failure(self):
        self.default_fcm_token
        # a relative survey 5 days in the past
        self.generate_easy_relative_schedule_event_with_relative_schedule(timedelta(days=5))
        self.assert_no_schedules()
    
    @patch("libs.push_notification_helpers.send_notification")
    def test_absolute_success(self, send_notification: MagicMock):
        send_notification.return_value = None
        
        self.default_fcm_token
        the_past = timezone.now() + timedelta(days=-5)
        # an absolute survey 5 days in the past
        schedule = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(the_past)
        self.assert_default_schedule_found(schedule)
    
    @time_machine.travel(THURS_OCT_6_NOON_2022_NY)
    def test_weekly_success(self):
        tz = gettz("America/New_York")
        self.default_study.update(timezone_name="America/New_York")
        
        self.default_fcm_token
        sched = self.generate_weekly_schedule(self.default_survey, 5, 0, 0)  # friday at midnight
        self.assertEqual(0, ScheduledEvent.objects.count())
        # THURS_OCT_6_NOON_2022_NY - this will create the midnight friday 7th and 14th of october
        repopulate_weekly_survey_schedule_events(self.default_survey, self.default_participant)
        self.assertEqual(2, ScheduledEvent.objects.count())
        s1, s2 = ScheduledEvent.objects.order_by("scheduled_time")
        
        with time_machine.travel(THURS_OCT_13_NOON_2022_NY):
            # this is what the test formerly would test but this is weird?
            self.assert_default_schedule_found(s1)
    
    @time_machine.travel(THURS_OCT_6_NOON_2022_NY)
    def test_weekly_future_explicit(self):
        tz = gettz("America/New_York")
        self.default_study.update(timezone_name="America/New_York")
        
        self.default_fcm_token
        sched = self.generate_weekly_schedule(self.default_survey, 5, 0, 0)  # friday at midnight
        self.assertEqual(0, ScheduledEvent.objects.count())
        repopulate_weekly_survey_schedule_events(self.default_survey, self.default_participant)
        self.assertEqual(2, ScheduledEvent.objects.count())
        
        midnight = dt_time(0, 0)
        friday = THURS_OCT_6_NOON_2022_NY.date() + timedelta(days=1)
        next_friday = THURS_OCT_6_NOON_2022_NY.date() + timedelta(days=8)
        push_time_1 = datetime.combine(friday, midnight, tz)
        push_time_2 = datetime.combine(next_friday, midnight, tz)
        
        s1, s2 = ScheduledEvent.objects.order_by("scheduled_time")
        self.assertEqual(s1.scheduled_time, push_time_1)
        self.assertEqual(s2.scheduled_time, push_time_2)
    
    @time_machine.travel(THURS_OCT_13_NOON_2022_NY)
    def test_participant_time_zones(self):
        self.default_fcm_token
        self.default_study.update(timezone_name='America/New_York')  # default in tests is normally UTC
        self.default_study.refresh_from_db()
        sched = self.generate_weekly_schedule(self.default_survey, 4, 12, 0)  # thursday at eleven...
        
        # need to time travel to the past to get the weekly logic to produce the correct time(s)
        with time_machine.travel(THURS_OCT_6_NOON_2022_NY):
            # creates a weekly survey for 2022-10-13 12:00:00-04:00
            self.assertEqual(0, ScheduledEvent.objects.count())
            repopulate_weekly_survey_schedule_events(self.default_survey, self.default_participant)
            # "this week"'s schedule time has already passed, so it does not get c
            # this occurs because of a <= in the logic, but it still shows us it is possible
            self.assertEqual(1, ScheduledEvent.objects.count())
            schedule = ScheduledEvent.objects.order_by("scheduled_time").first()
        
        # this makes it easier to identify if something is actually wrong than the datetime
        # comparison_time the database normalizes the timezone to UTC
        comparison_time = schedule.scheduled_time.astimezone(gettz("America/New_York"))
        self.assertEqual(comparison_time.year, 2022)
        self.assertEqual(comparison_time.month, 10)
        self.assertEqual(comparison_time.day, 13)
        self.assertEqual(comparison_time.hour, 12)
        self.assertEqual(comparison_time.minute, 0)
        self.assertEqual(comparison_time.second, 0)
        self.assertEqual(comparison_time.tzinfo, gettz("America/New_York"))
        
        # set default participant to pacific time, assert that no push notification is calculated.
        self.default_participant.try_set_timezone('America/Los_Angeles')
        self.assert_no_schedules()
        
        # set the time zone to mountain time, assert that no push notification is calculated.
        self.default_participant.try_set_timezone('America/Denver')
        self.assert_no_schedules()
        
        # set the time zone to central time, assert that no push notification is calculated.
        self.default_participant.try_set_timezone('America/Chicago')
        self.assert_no_schedules()
        
        # but if you set the time zone to New_York the push notification is calculated!
        self.default_participant.try_set_timezone('America/New_York')
        self.assert_default_schedule_found(schedule)
    
    @time_machine.travel(WEDNESDAY_JUNE_NOON_8_2022_EDT)
    def test_no_timezone_bug(self):
        plus_one_hour = WEDNESDAY_JUNE_NOON_8_2022_EDT + timedelta(hours=1)
        minus_one_hour = WEDNESDAY_JUNE_NOON_8_2022_EDT - timedelta(hours=1)
        minus_two_hours = WEDNESDAY_JUNE_NOON_8_2022_EDT - timedelta(hours=2)
        minus_three_hours = WEDNESDAY_JUNE_NOON_8_2022_EDT - timedelta(hours=3)
        minus_four_hours = WEDNESDAY_JUNE_NOON_8_2022_EDT - timedelta(hours=4)
        minus_five_hours = WEDNESDAY_JUNE_NOON_8_2022_EDT - timedelta(hours=5)
        # GMT_time = WEDNESDAY_JUNE_NOON_8_2022_EDT.replace(tzinfo=gettz('GMT'))
        self.default_fcm_token
        
        # we have a bug where if the participant's timezone shifts it into the past it will not be
        # noticed by the survey schedule query until the study-timezone-based ScheduledEvent time is
        # in the past (has already passed).
        # Testing this with an absolute schedule.
        self.default_study.update_only(timezone_name='America/New_York')
        scheduled_event = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(WEDNESDAY_JUNE_NOON_8_2022_EDT)
        absolute_schedule = AbsoluteSchedule.obj_get()  # will fail if there is more than one, which would be a bug
        
        self.default_participant.try_set_timezone('GMT')
        self.default_participant.refresh_from_db()
        self.assertEqual(self.default_participant.timezone_name, 'GMT')
        
        # EDT is +4 hours of GMT, so the schedule "should trigger" at 12:00 GMT, but instead
        # triggers at 12:00 EDT, because the participant's timezone incorrectly checked.
        
        with time_machine.travel(plus_one_hour):
            self.assert_default_schedule_found(scheduled_event)
        
        with time_machine.travel(minus_one_hour):
            self.assert_default_schedule_found(scheduled_event)
        
        with time_machine.travel(minus_two_hours):
            self.assert_default_schedule_found(scheduled_event)
        
        with time_machine.travel(minus_three_hours):
            self.assert_default_schedule_found(scheduled_event)
        
        with time_machine.travel(minus_four_hours):
            self.assert_default_schedule_found(scheduled_event)
        
        with time_machine.travel(minus_five_hours):
            self.assert_no_schedules()
    
    # using weekly as a base we now test situations where it shouldn't return schedules
    @time_machine.travel(THURS_OCT_6_NOON_2022_NY)
    def test_deleted_hidden_study(self):
        self.default_fcm_token
        
        # schedules = self.generate_a_real_weekly_schedule_event_with_schedule(5)
        sched = self.generate_weekly_schedule(self.default_survey, 5, 0, 0)  # friday, midnight
        repopulate_weekly_survey_schedule_events(self.default_survey)
        self.assertEqual(ScheduledEvent.objects.count(), 2)
        
        self.default_study.update(deleted=True)
        with time_machine.travel(THURS_OCT_20_NOON_2022_NY):
            self.assert_no_schedules()
        
        # and assert that schedules are removed this study after it is deleted?
        self.assertEqual(ScheduledEvent.objects.count(), 2)
        push_notification_scheduledevent_rebuild()
        self.assertEqual(ScheduledEvent.objects.count(), 0)
    
    @time_machine.travel(THURS_OCT_6_NOON_2022_NY)
    def test_manually_stopped_study(self):
        self.default_fcm_token
        sched = self.generate_weekly_schedule(self.default_survey, 5, 0, 0)  # friday, midnight
        repopulate_weekly_survey_schedule_events(self.default_survey)
        self.assertEqual(ScheduledEvent.objects.count(), 2)
        
        self.default_study.update(manually_stopped=True)
        with time_machine.travel(THURS_OCT_20_NOON_2022_NY):
            self.assert_no_schedules()
        
        self.assertEqual(ScheduledEvent.objects.count(), 2)
        push_notification_scheduledevent_rebuild()
        self.assertEqual(ScheduledEvent.objects.count(), 0)
    
    @time_machine.travel(THURS_OCT_6_NOON_2022_NY)
    def test_past_end_date(self):
        self.default_fcm_token
        sched = self.generate_weekly_schedule(self.default_survey, 5, 0, 0)  # friday, midnight
        repopulate_weekly_survey_schedule_events(self.default_survey)
        self.assertEqual(ScheduledEvent.objects.count(), 2)
        
        # not testing time zones, just testing end date
        self.default_study.update(end_date=timezone.now().date() - timedelta(days=10))
        with time_machine.travel(THURS_OCT_20_NOON_2022_NY):
            self.assert_no_schedules()
        
        self.assertEqual(ScheduledEvent.objects.count(), 2)
        push_notification_scheduledevent_rebuild()
        self.assertEqual(ScheduledEvent.objects.count(), 0)


class SchedulePersistenceCheck:
    """ A helper class providing a pattern for testing that new identical scheduled events are not
    replaced, are instead allowed to remain. """
    
    def type_nonce(self, wkly_sched, rltv_sched, abslt_sched):
        if wkly_sched:
            return f"weekly {wkly_sched}"
        elif rltv_sched:
            return f"relative {rltv_sched}"
        elif abslt_sched:
            return f"absolute {abslt_sched}"
        else:
            raise BaseException("literally impossible a scheduled event must have a schedule")
    
    @property
    def setup_assert_new_scheduled_events_dont_replace_existing(self):
        """ Run this before a repopulate is called. """
        
        q = ScheduledEvent.objects.values_list(*SCHEDULEDEVENT_IDENTITY_FIELDS)
        assert q.count() != 0, "Incorrect usage, no scheduled events present"
        # print("before count:", ScheduledEvent.objects.count())
        self.before_state = {}
        
        # we use participant id, survey id, scheduled time, and the type of schedule to uniquely
        # identify each scheduled event, so we can identify if the primary key, uuid, or uuid changed
        for (part_id, surv_id, scheduled_time, wkly_sched, rltv_sched, abslt_sched, pk, uuid) in q:
            # print("part_id:", part_id, "surv_id:", surv_id, "scheduled_time:", scheduled_time, "wkly_sched:", wkly_sched, "rltv_sched:", rltv_sched, "abslt_sched:", abslt_sched, "pk:", pk, "uuid:", uuid,)
            type_nonce = self.type_nonce(wkly_sched, rltv_sched, abslt_sched)
            key = (part_id, surv_id, scheduled_time, type_nonce)
            value = (pk, uuid)
            
            # should be literally impossible to have a duplicate
            if key in self.before_state:
                raise Exception(f"Encountered duplicate key in test setup: {key}")
            
            self.before_state[key] = value
        # print(f"before_state: {self.before_state}")
    
    @property
    def assert_new_scheduled_events_dont_replace_existing(self):
        """ Tests that any identical scheduled events from the before time are not replaced. """
        
        assert hasattr(self, "before_state"), "did not set up 'before_state' in test"
        # print("after count:", ScheduledEvent.objects.count())
        after_state = {}
        
        q = ScheduledEvent.objects.values_list(*SCHEDULEDEVENT_IDENTITY_FIELDS)
        for (part_id, surv_id, scheduled_time, wkly_sched, rltv_sched, abslt_sched, pk, uuid) in q:
            # print("part_id:", part_id, "surv_id:", surv_id, "scheduled_time:", scheduled_time, "wkly_sched:", wkly_sched, "rltv_sched:", rltv_sched, "abslt_sched:", abslt_sched, "pk:", pk, "uuid:", uuid,)
            type_nonce = self.type_nonce(wkly_sched, rltv_sched, abslt_sched)
            key = (part_id, surv_id, scheduled_time, type_nonce)
            new_value = (pk, uuid)
            
            if key not in self.before_state:
                continue  # only test scheduled events that existed previously.
            
            # (must be a subclass of test case)
            self.assertEqual(self.before_state[key], new_value)
            after_state[key] = new_value
        
        # print(f"after_state: {after_state}")



class TestWeeklyTimingsSchedules(CommonTestCase):
    
    def test_immutable_defaults(self):
        # assert that this variable creates lists anew.
        self.assertIsNot(EMPTY_WEEKLY_SURVEY_TIMINGS(), EMPTY_WEEKLY_SURVEY_TIMINGS())
    
    def test_export_weekly_survey_timings_no_schedules(self):
        # assert function only works with populated weekly schedules
        with self.assertRaises(NoSchedulesException):
            get_next_weekly_event_and_schedule(self.default_survey)
    
    def test_export_weekly_survey_timings(self):
        # assert that the timings output from no-schedules survey are the empty timings dict
        self.assertEqual(
            EMPTY_WEEKLY_SURVEY_TIMINGS(), export_weekly_survey_timings(self.default_survey)
        )
    
    def test_each_day_of_week(self):
        # test that each weekday
        timings = EMPTY_WEEKLY_SURVEY_TIMINGS()
        for day_of_week in range(0, 7):
            self.generate_weekly_schedule(self.default_survey, day_of_week=day_of_week)
            timings[day_of_week].append(0)  # time of day defaults to zero
        # assert there are 7 weekly surveys, that they are one per day, at midnight (0)
        self.assertEqual(WeeklySchedule.objects.count(), 7)
        self.assertEqual(timings, MIDNIGHT_EVERY_DAY_OF_WEEK())
        self.assertEqual(timings, export_weekly_survey_timings(self.default_survey))
    
    def test_create_weekly_schedules(self):
        # assert we handle no surveys case
        WeeklySchedule.configure_weekly_schedules(EMPTY_WEEKLY_SURVEY_TIMINGS(), self.default_survey)
        self.assertEqual(WeeklySchedule.objects.count(), 0)
        # assert we created a survey for every week
        WeeklySchedule.configure_weekly_schedules(MIDNIGHT_EVERY_DAY_OF_WEEK(), self.default_survey)
        self.assertEqual(WeeklySchedule.objects.count(), 7)
        self.assertEqual(
            sorted(list(WeeklySchedule.objects.values_list("day_of_week", flat=True))),
            list(range(0, 7)),
        )
    
    def test_create_weekly_schedules_time_details(self):
        timings = EMPTY_WEEKLY_SURVEY_TIMINGS()
        timings[0].append(3600 + 120)  # schedule 1am and 2 minutes on sunday
        WeeklySchedule.configure_weekly_schedules(timings, self.default_survey)
        self.assertEqual(WeeklySchedule.objects.count(), 1)
        weekly = WeeklySchedule.objects.first()
        self.assertEqual(weekly.day_of_week, 0)
        self.assertEqual(weekly.hour, 1)
        self.assertEqual(weekly.minute, 2)
    
    def test_create_weekly_schedules_time_details_2(self):
        # as test_create_weekly_schedules_details, but we drop seconds because we only have minutes
        timings = EMPTY_WEEKLY_SURVEY_TIMINGS()
        timings[0].append(3600 + 120 + 1)  # schedule 1am and 1 minute and 1 second on sunday
        WeeklySchedule.configure_weekly_schedules(timings, self.default_survey)
        self.assertEqual(WeeklySchedule.objects.count(), 1)
        weekly = WeeklySchedule.objects.first()
        self.assertEqual(weekly.day_of_week, 0)
        self.assertEqual(weekly.hour, 1)
        self.assertEqual(weekly.minute, 2)
    
    def test_create_weekly_schedules_bad_count(self):
        # for lengths of lists of ints 1-10 assert that the appropriate error is raised
        for i in range(1, 10):
            timings = [[0] for _ in range(i)]
            if len(timings) != 7:
                self.assertRaises(
                    BadWeeklyCount, WeeklySchedule.configure_weekly_schedules, timings, self.default_survey
                )
            else:
                WeeklySchedule.configure_weekly_schedules(timings, self.default_survey)
                self.assertEqual(WeeklySchedule.objects.count(), 7)
    
    def assert_is_just_weekly_midnight(self):
        self.assertEqual(WeeklySchedule.objects.count(), 7)
        for i, sched in enumerate(WeeklySchedule.objects.order_by("day_of_week")):
            self.assertEqual(sched.day_of_week, i)
            self.assertEqual(sched.hour, 0)
            self.assertEqual(sched.minute, 0)
    
    def test_create_weekly_clears(self):
        # test that deleted surveys and empty timings lists delete stuff
        WeeklySchedule.configure_weekly_schedules(MIDNIGHT_EVERY_DAY_OF_WEEK(), self.default_survey)
        self.assert_is_just_weekly_midnight()
        # test empty case
        WeeklySchedule.configure_weekly_schedules([], self.default_survey)
        self.assertEqual(WeeklySchedule.objects.count(), 0)
        WeeklySchedule.configure_weekly_schedules(MIDNIGHT_EVERY_DAY_OF_WEEK(), self.default_survey)
        self.assert_is_just_weekly_midnight()
        # test deleted survey case
        self.default_survey.update(deleted=True)
        WeeklySchedule.configure_weekly_schedules(MIDNIGHT_EVERY_DAY_OF_WEEK(), self.default_survey)
        self.assertEqual(WeeklySchedule.objects.count(), 0)
        self.default_survey.update(deleted=False)
        WeeklySchedule.configure_weekly_schedules(MIDNIGHT_EVERY_DAY_OF_WEEK(), self.default_survey)
        self.assert_is_just_weekly_midnight()
    
    def test_create_weekly_does_not_delete_existing(self):
        timings = MIDNIGHT_EVERY_DAY_OF_WEEK()  #[[0], [0], [0], [0], [0], [0], [0]]
        WeeklySchedule.configure_weekly_schedules(timings, self.default_survey)
        self.assert_is_just_weekly_midnight()
        pks = set(WeeklySchedule.objects.values_list("pk", flat=True))
        timings[0].append(3600) # add a 1am
        WeeklySchedule.configure_weekly_schedules(timings, self.default_survey)
        self.assertEqual(WeeklySchedule.objects.count(), 8)
        pks_2 = set(WeeklySchedule.objects.values_list("pk", flat=True))
        self.assertEqual(len(pks_2 - pks), 1)
        extra_pk = (pks_2 - pks).pop()
        extra_sched = WeeklySchedule.objects.get(pk=extra_pk)
        self.assertEqual(extra_sched.day_of_week, 0)
        self.assertEqual(extra_sched.hour, 1)
        self.assertEqual(extra_sched.minute, 0)
    
    def test_create_weekly_deduplicates(self):
        timings = MIDNIGHT_EVERY_DAY_OF_WEEK()
        timings[0].append(0)
        WeeklySchedule.configure_weekly_schedules(timings, self.default_survey)
        self.assert_is_just_weekly_midnight()
        pks = set(WeeklySchedule.objects.values_list("pk", flat=True))
        
        # and then aggain because updating in place should not create duplicates
        timings = MIDNIGHT_EVERY_DAY_OF_WEEK()  # (should be irrelevant)
        timings[0].append(0)
        WeeklySchedule.configure_weekly_schedules(timings, self.default_survey)
        pks2 = set(WeeklySchedule.objects.values_list("pk", flat=True))
        self.assertEqual(pks, pks2)
    
    def test_create_weekly_deletes_correctly(self):
        timings = MIDNIGHT_EVERY_DAY_OF_WEEK()
        WeeklySchedule.configure_weekly_schedules(timings, self.default_survey)
        first = WeeklySchedule.objects.order_by("day_of_week").first()
        last = WeeklySchedule.objects.order_by("day_of_week").last()
        self.assertEqual(first.day_of_week, 0)
        self.assertEqual(last.day_of_week, 6)
        timings[0].pop()
        WeeklySchedule.configure_weekly_schedules(timings, self.default_survey)
        first = WeeklySchedule.objects.order_by("day_of_week").first()
        last = WeeklySchedule.objects.order_by("day_of_week").last()
        self.assertEqual(first.day_of_week, 1)
        self.assertEqual(last.day_of_week, 6)


class TestRelativeSchedulesCreation(CommonTestCase):
    
    @property
    def one_day_one_hour(self):
        return (self.default_intervention.pk, 1, 3600)
    
    @property
    def two_days_two_hours(self):
        return (self.default_intervention.pk, 2, 7200)
    
    def assert_one_day_one_hour(self, r: RelativeSchedule):
        self.assertEqual(r.intervention_id, self.default_intervention.pk)
        self.assertEqual(r.days_after, 1)
        self.assertEqual(r.hour, 1)
    
    def assert_two_days_two_hours(self, r: RelativeSchedule):
        self.assertEqual(r.intervention_id, self.default_intervention.pk)
        self.assertEqual(r.days_after, 2)
        self.assertEqual(r.hour, 2)
    
    def test_create_one_relative_schedules(self):
        RelativeSchedule.configure_relative_schedules([self.one_day_one_hour], self.default_survey)
        self.assertEqual(RelativeSchedule.objects.count(), 1)
        self.assert_one_day_one_hour(RelativeSchedule.obj_get())
    
    def test_create_two_relative_schedules(self):
        RelativeSchedule.configure_relative_schedules([self.one_day_one_hour, self.two_days_two_hours], self.default_survey)
        self.assertEqual(RelativeSchedule.objects.count(), 2)
        one, two = RelativeSchedule.objects.order_by("hour").all()
        self.assert_one_day_one_hour(one)
        self.assert_two_days_two_hours(two)
    
    def test_create_relative_schedules_clears(self):
        RelativeSchedule.configure_relative_schedules([self.one_day_one_hour], self.default_survey)
        self.assertEqual(RelativeSchedule.objects.count(), 1)  # need populated
        RelativeSchedule.configure_relative_schedules([], self.default_survey)  # empty case
        self.assertEqual(RelativeSchedule.objects.count(), 0)
        RelativeSchedule.configure_relative_schedules([self.one_day_one_hour], self.default_survey)
        self.assertEqual(RelativeSchedule.objects.count(), 1)  # need populated
        self.default_survey.update(deleted=True)  # ensure deleted surveys are cleared
        RelativeSchedule.configure_relative_schedules([self.one_day_one_hour], self.default_survey)
        self.assertEqual(RelativeSchedule.objects.count(), 0)
    
    def test_create_relative_schedules_deduplicates(self):
        RelativeSchedule.configure_relative_schedules([self.one_day_one_hour], self.default_survey)
        self.assertEqual(RelativeSchedule.objects.count(), 1)
        RelativeSchedule.configure_relative_schedules([self.one_day_one_hour], self.default_survey)
        self.assertEqual(RelativeSchedule.objects.count(), 1)
        RelativeSchedule.objects.all().delete()
        RelativeSchedule.configure_relative_schedules(
            [self.one_day_one_hour, self.one_day_one_hour], self.default_survey)
        self.assertEqual(RelativeSchedule.objects.count(), 1)
    
    def test_create_relative_schedules_does_not_delete_existing(self):
        RelativeSchedule.configure_relative_schedules([self.one_day_one_hour], self.default_survey)
        one = RelativeSchedule.obj_get()
        RelativeSchedule.configure_relative_schedules(
            [self.one_day_one_hour, self.two_days_two_hours], self.default_survey)
        self.assertEqual(RelativeSchedule.objects.count(), 2)
        one_again, two = RelativeSchedule.objects.order_by("created_on")
        self.assertEqual(one.pk, one_again.pk)
        self.assertEqual(one.hour, one_again.hour)
        self.assertEqual(one.days_after, one_again.days_after)
        self.assertEqual(one.hour, one_again.hour)
        self.assertEqual(one.minute, one_again.minute)
        self.assert_one_day_one_hour(one)
        self.assert_one_day_one_hour(one_again)
        self.assert_two_days_two_hours(two)
        # test it deletes the correct one
        RelativeSchedule.configure_relative_schedules([self.two_days_two_hours], self.default_survey)
        self.assertEqual(RelativeSchedule.objects.count(), 1)
        two_again = RelativeSchedule.obj_get()
        self.assert_two_days_two_hours(two_again)
        self.assertEqual(two.pk, two_again.pk)
        self.assertEqual(two.hour, two_again.hour)
        self.assertEqual(two.days_after, two_again.days_after)
        self.assertEqual(two.hour, two_again.hour)
        self.assertEqual(two.minute, two_again.minute)


class TestAbsoluteSchedulesCreation(CommonTestCase):
    TODAY = timezone.now().today().date()
    TOMORROW = TODAY + timedelta(days=1)
    
    @property
    def today_at_one(self):
        return self.TODAY.year, self.TODAY.month, self.TODAY.day, 3600
    
    @property
    def tomorrow_at_two(self):
        return self.TOMORROW.year, self.TOMORROW.month, self.TOMORROW.day, 3600*2
    
    def test_create_absolute_schedule(self):
        AbsoluteSchedule.configure_absolute_schedules([self.today_at_one], self.default_survey)
        self.assertEqual(AbsoluteSchedule.objects.count(), 1)
        AbsoluteSchedule.obj_get()
        self.assertEqual(AbsoluteSchedule.obj_get().date, self.TODAY)
        self.assertEqual(AbsoluteSchedule.obj_get().hour, 1)
    
    def test_create_two_absolute_schedules(self):
        AbsoluteSchedule.configure_absolute_schedules([self.today_at_one, self.tomorrow_at_two], self.default_survey)
        self.assertEqual(AbsoluteSchedule.objects.count(), 2)
        one, two = AbsoluteSchedule.objects.order_by("date")
        self.assertEqual(one.date, self.TODAY)
        self.assertEqual(one.hour, 1)
        self.assertEqual(two.date, self.TOMORROW)
        self.assertEqual(two.hour, 2)
    
    def test_create_absolute_schedule_clears(self):
        AbsoluteSchedule.configure_absolute_schedules([self.today_at_one], self.default_survey)
        self.assertEqual(AbsoluteSchedule.objects.count(), 1)
        AbsoluteSchedule.configure_absolute_schedules([], self.default_survey)
        self.assertEqual(AbsoluteSchedule.objects.count(), 0)
        AbsoluteSchedule.configure_absolute_schedules([self.today_at_one], self.default_survey)
        self.assertEqual(AbsoluteSchedule.objects.count(), 1)
        self.default_survey.update(deleted=True)
        AbsoluteSchedule.configure_absolute_schedules([self.today_at_one], self.default_survey)
        self.assertEqual(AbsoluteSchedule.objects.count(), 0)
    
    def test_create_absolute_schedule_deduplicates(self):
        # identical schedules should not be duplicated
        AbsoluteSchedule.configure_absolute_schedules([self.today_at_one], self.default_survey)
        self.assertEqual(AbsoluteSchedule.objects.count(), 1)
        AbsoluteSchedule.configure_absolute_schedules([self.today_at_one], self.default_survey)
        self.assertEqual(AbsoluteSchedule.objects.count(), 1)
        AbsoluteSchedule.objects.all().delete()
        # should result in a single schedule
        AbsoluteSchedule.configure_absolute_schedules([self.today_at_one, self.today_at_one], self.default_survey)
        self.assertEqual(AbsoluteSchedule.objects.count(), 1)
        # should create only one new schedule
        AbsoluteSchedule.configure_absolute_schedules([self.today_at_one, self.tomorrow_at_two], self.default_survey)
        self.assertEqual(AbsoluteSchedule.objects.count(), 2)
    
    def test_create_absolute_schedule_does_not_delete_existing(self):
        AbsoluteSchedule.configure_absolute_schedules([self.today_at_one], self.default_survey)
        one = AbsoluteSchedule.obj_get()
        AbsoluteSchedule.configure_absolute_schedules([self.today_at_one, self.tomorrow_at_two], self.default_survey)
        self.assertEqual(AbsoluteSchedule.objects.count(), 2)
        one_again, two = AbsoluteSchedule.objects.order_by("date")
        self.assertEqual(one.hour, 1)
        self.assertEqual(one.pk, one_again.pk)
        self.assertEqual(one.date, one_again.date)
        self.assertEqual(one.hour, one_again.hour)
        # and then test that it deletes the correct one
        AbsoluteSchedule.configure_absolute_schedules([self.tomorrow_at_two], self.default_survey)
        self.assertEqual(AbsoluteSchedule.objects.count(), 1)
        two_again = AbsoluteSchedule.obj_get()
        self.assertEqual(two.pk, two_again.pk)
        self.assertEqual(two.date, self.TOMORROW)
        self.assertEqual(two_again.date, self.TOMORROW)
        self.assertEqual(two.hour, 2)
        self.assertEqual(two_again.hour, 2)
    


class TestEventCreation(CommonTestCase, SchedulePersistenceCheck):
    # originally started as copy of TestGetLatestSurveys in test_mobile_endpoints.py
    
    @staticmethod
    def assert_is_a_week_in_correct_timezone_period(
        week: List[datetime], timezone: tz.tzfile, tz_subperiod_str: str
    ):
        # test that the data our fake data actually matches what we want
        for dt in week:
            assert dt.tzinfo == timezone
            assert tz_subperiod_str == dt.strftime("%Z")  # tz as name of timezone
            assert dt.hour == 12
        
        # assert that these are sequential days starting on a monday testing each day of the week
        for i in range(0, 7):
            assert week[i].date() == week[0].date() + timedelta(days=i)
            assert week[0].weekday() == 0
    
    def test_weeks_are_in_correct_timezones(self):
        # test that the weeks are in the correct timezones
        eastern = tz.gettz("America/New_York")
        self.assert_is_a_week_in_correct_timezone_period(EST_WEEK, eastern, "EST")
        self.assert_is_a_week_in_correct_timezone_period(EDT_WEEK, eastern, "EDT")
    
    #
    ## helper functions
    #
    
    def create_weekly_midnight(self):
        WeeklySchedule.configure_weekly_schedules(MIDNIGHT_EVERY_DAY_OF_WEEK(), self.default_survey)
    
    def create_weekly_noon(self):
        WeeklySchedule.configure_weekly_schedules(NOON_EVERY_DAY_OF_WEEK(), self.default_survey)
    
    def iterate_weekday_absolute_schedules(self):
        # iterates over days of the week and populates absolute schedules and scheduled events
        start, _ = get_start_and_end_of_java_timings_week(timezone.now())
        for i in range(0, 7):
            AbsoluteSchedule.objects.all().delete()
            ScheduledEvent.objects.all().delete()
            a_date = start.date() + timedelta(days=i)
            self.generate_absolute_schedule(a_date)
            repopulate_absolute_survey_schedule_events(
                self.default_survey, self.default_participant
            )
            # correct weekday for sunday-zero-index
            yield (a_date.weekday() + 1) % 7
    
    @property
    def generate_a_valid_schedule_of_each_type(self):
        # valid here meaning it should create a scheduled event on a valid participant
        self.default_populated_intervention_date
        self.generate_relative_schedule(self.default_survey, self.default_intervention, days_after=0)
        self.generate_weekly_schedule(self.default_survey, day_of_week=0)  # monday
        self.generate_absolute_schedule(timezone.now().date())
    
    @property
    def assert_one_of_each_scheduled_event(self):
        self.assertEqual(ScheduledEvent.objects.count(), 3)
        self.assertEqual(ScheduledEvent.objects.filter(weekly_schedule__isnull=False).count(), 1)
        self.assertEqual(ScheduledEvent.objects.filter(relative_schedule__isnull=False).count(), 1)
        self.assertEqual(ScheduledEvent.objects.filter(absolute_schedule__isnull=False).count(), 1)
    
    @property
    def assert_no_scheduled_events(self):
        def info():
            weekly_schedule = ScheduledEvent.objects.filter(weekly_schedule__isnull=False).count()
            relative_schedule = ScheduledEvent.objects.filter(relative_schedule__isnull=False).count()
            absolute_schedule = ScheduledEvent.objects.filter(absolute_schedule__isnull=False).count()
            return f"weekly_schedules: {weekly_schedule}, relative_schedules: {relative_schedule}, absolute_schedules: {absolute_schedule}"
        assert ScheduledEvent.objects.count() == 0, "ASSERT_NO_SCHEDULED_EVENTS: " + info()
    
    @time_machine.travel(THURS_OCT_6_NOON_2022_NY)
    def test_that_iteration_function(self):
        # test for absolute surveys that they show up regardless of the day of the week they fall on,
        # as long as that day is within the current week.
        self.default_survey
        for i, day_of_week_index in enumerate(self.iterate_weekday_absolute_schedules()):
            # week_day = i % 4  # correct for it being thursday
            # print(i, week_day, day_of_week_index)
            self.assertEqual(i, day_of_week_index)
        self.assertEqual(i, 6)  # this should fail it should be 6
    
    #
    ## basic event creation tests
    #
    
    def test_relative_survey_basic_event_generation(self):
        self.default_survey, self.default_participant
        # make sure test infrastructure does not have weird database side effects
        self.assertEqual(RelativeSchedule.objects.count(), 0)
        self.assert_no_scheduled_events
        self.generate_relative_schedule(self.default_survey, self.default_intervention, days_after=0)
        repopulate_relative_survey_schedule_events(self.default_survey, self.default_participant)
        self.assertEqual(RelativeSchedule.objects.count(), 1)  # change
        self.assert_no_scheduled_events    # no change
        self.default_populated_intervention_date
        self.assertEqual(RelativeSchedule.objects.count(), 1)  # no change
        self.assert_no_scheduled_events    # no change
        repopulate_relative_survey_schedule_events(self.default_survey, self.default_participant)
        self.assertEqual(RelativeSchedule.objects.count(), 1)  # no change
        self.assertEqual(ScheduledEvent.objects.count(), 1)    # change
        self.assertEqual(ArchivedEvent.objects.count(), 0)
        self.setup_assert_new_scheduled_events_dont_replace_existing
        repopulate_relative_survey_schedule_events(self.default_survey, self.default_participant)
        self.assert_new_scheduled_events_dont_replace_existing
        self.assertEqual(RelativeSchedule.objects.count(), 1)  # no change
        self.assertEqual(ScheduledEvent.objects.count(), 1)    # change
        self.assertEqual(ArchivedEvent.objects.count(), 0)
    
    def test_relative_survey_basic_event_generation_no_participant_in_call(self):
        self.default_populated_intervention_date
        self.generate_relative_schedule(self.default_survey, self.default_intervention, days_after=0)
        repopulate_relative_survey_schedule_events(self.default_survey)
        self.assertEqual(ScheduledEvent.objects.count(), 1)
        self.assertEqual(RelativeSchedule.objects.count(), 1)
        self.setup_assert_new_scheduled_events_dont_replace_existing
        repopulate_relative_survey_schedule_events(self.default_survey)
        self.assert_new_scheduled_events_dont_replace_existing
        self.assertEqual(ScheduledEvent.objects.count(), 1)
        self.assertEqual(RelativeSchedule.objects.count(), 1)
    
    def test_absolute_schedule_basic_event_generation(self):
        self.default_survey, self.default_participant
        # make sure test infrastructure does not have weird database side effects
        self.assertEqual(AbsoluteSchedule.objects.count(), 0)
        self.assert_no_scheduled_events
        self.generate_absolute_schedule(timezone.now().date())
        self.assertEqual(AbsoluteSchedule.objects.count(), 1) # change
        self.assert_no_scheduled_events   # no change
        repopulate_absolute_survey_schedule_events(self.default_survey, self.default_participant)
        self.assertEqual(AbsoluteSchedule.objects.count(), 1) # no change
        self.assertEqual(ScheduledEvent.objects.count(), 1)   # change
        self.assertEqual(ArchivedEvent.objects.count(), 0)
        self.setup_assert_new_scheduled_events_dont_replace_existing
        repopulate_absolute_survey_schedule_events(self.default_survey, self.default_participant)
        self.assert_new_scheduled_events_dont_replace_existing
        self.assertEqual(AbsoluteSchedule.objects.count(), 1) # no change
        self.assertEqual(ScheduledEvent.objects.count(), 1)   # change
        self.assertEqual(ArchivedEvent.objects.count(), 0)
    
    def test_absolute_schedule_basic_event_generation_no_participant_in_call(self):
        self.using_default_participant()
        self.generate_absolute_schedule(timezone.now().date())
        repopulate_absolute_survey_schedule_events(self.default_survey)
        self.assertEqual(ScheduledEvent.objects.count(), 1)
        self.assertEqual(ArchivedEvent.objects.count(), 0)
        self.setup_assert_new_scheduled_events_dont_replace_existing
        repopulate_absolute_survey_schedule_events(self.default_survey)
        self.assert_new_scheduled_events_dont_replace_existing
        self.assertEqual(ScheduledEvent.objects.count(), 1)
        self.assertEqual(ArchivedEvent.objects.count(), 0)
    
    def test_weekly_schedule_basic_event_generation(self):
        self.default_survey, self.default_participant
        # make sure test infrastructure does not have weird database side effects
        self.assertEqual(WeeklySchedule.objects.count(), 0)
        self.assert_no_scheduled_events
        self.generate_weekly_schedule(self.default_survey, day_of_week=0)  # monday
        self.assertEqual(WeeklySchedule.objects.count(), 1)  # change
        self.assert_no_scheduled_events  # no change
        repopulate_weekly_survey_schedule_events(self.default_survey, self.default_participant)
        self.assertEqual(WeeklySchedule.objects.count(), 1)  # no change
        self.assertEqual(ScheduledEvent.objects.count(), 1)  # change
        self.assertEqual(ArchivedEvent.objects.count(), 0)
        self.setup_assert_new_scheduled_events_dont_replace_existing
        repopulate_weekly_survey_schedule_events(self.default_survey, self.default_participant)
        self.assert_new_scheduled_events_dont_replace_existing
        self.assertEqual(WeeklySchedule.objects.count(), 1)  # no change
        self.assertEqual(ScheduledEvent.objects.count(), 1)  # change
        self.assertEqual(ArchivedEvent.objects.count(), 0)
    
    def test_weekly_schedule_basic_event_generation_no_participant_in_call(self):
        self.generate_weekly_schedule(self.default_survey, day_of_week=0)  # monday
        self.using_default_participant()
        repopulate_weekly_survey_schedule_events(self.default_survey)
        self.assertEqual(WeeklySchedule.objects.count(), 1)
        self.assertEqual(ArchivedEvent.objects.count(), 0)
        self.setup_assert_new_scheduled_events_dont_replace_existing
        repopulate_weekly_survey_schedule_events(self.default_survey)
        self.assert_new_scheduled_events_dont_replace_existing
        self.assertEqual(WeeklySchedule.objects.count(), 1)
        self.assertEqual(ArchivedEvent.objects.count(), 0)
    
    def test_all_schedules_basic_event_generation(self):
        self.assertEqual(Survey.objects.count(), 0)
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assertEqual(Survey.objects.count(), 0)
        self.assertEqual(AbsoluteSchedule.objects.count(), 0)
        self.assertEqual(RelativeSchedule.objects.count(), 0)
        self.assertEqual(WeeklySchedule.objects.count(), 0)
        self.assert_no_scheduled_events
        # one last test of no database side effects...
        self.default_survey
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assertEqual(Survey.objects.count(), 1)
        self.assertEqual(AbsoluteSchedule.objects.count(), 0)
        self.assertEqual(RelativeSchedule.objects.count(), 0)
        self.assertEqual(WeeklySchedule.objects.count(), 0)
        self.assert_no_scheduled_events
        self.assertEqual(Intervention.objects.count(), 0)
        self.assertEqual(InterventionDate.objects.count(), 0)
        # test that the default schedules are created, then all 3 ScheduledEvent types
        self.generate_a_valid_schedule_of_each_type
        self.assert_no_scheduled_events
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assert_one_of_each_scheduled_event
        self.assertEqual(ArchivedEvent.objects.count(), 0)
        self.setup_assert_new_scheduled_events_dont_replace_existing
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assert_one_of_each_scheduled_event
        self.assert_new_scheduled_events_dont_replace_existing
        self.assertEqual(ArchivedEvent.objects.count(), 0)
    
    def test_repopulate_all_basic_event_generation_no_participant_in_call(self):
        self.generate_a_valid_schedule_of_each_type
        self.assert_no_scheduled_events
        repopulate_all_survey_scheduled_events(self.default_study)
        self.assert_one_of_each_scheduled_event
    
    def test_repopulate_doesnt_even_need_to_delete_scheduledevents_from_deleted_schedules(self):
        self.generate_a_valid_schedule_of_each_type
        repopulate_all_survey_scheduled_events(self.default_study)
        self.assert_one_of_each_scheduled_event
        WeeklySchedule.objects.all().delete()
        self.assertEqual(ScheduledEvent.objects.filter(weekly_schedule__isnull=False).count(), 0)
    
    def test_deleted_survey_deletes_scheduled_events(self):
        self.generate_a_valid_schedule_of_each_type
        repopulate_all_survey_scheduled_events(self.default_study)
        self.assert_one_of_each_scheduled_event
        self.default_survey.update(deleted=True)
        repopulate_all_survey_scheduled_events(self.default_study)
        self.assert_no_scheduled_events
    
    def test_deleted_participant_deletes_scheduled_events(self):
        self.generate_a_valid_schedule_of_each_type
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assert_one_of_each_scheduled_event
        self.default_participant.update(deleted=True)
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assert_no_scheduled_events
    
    def test_permanently_retired_participant_deletes_scheduled_events(self):
        self.generate_a_valid_schedule_of_each_type
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assert_one_of_each_scheduled_event
        self.default_participant.update(permanently_retired=True)
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assert_no_scheduled_events
    
    #
    ## test conditions where ScheduledEvents should not be created
    #
    
    def test_deleted_survey(self):
        self.default_survey.update(deleted=True)
        self.generate_a_valid_schedule_of_each_type
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assert_no_scheduled_events
        repopulate_all_survey_scheduled_events(self.default_study)  # always test both arg setups
        self.assert_no_scheduled_events
    
    def test_deleted_participant_gets_no_schedules(self):
        self.default_participant.update(deleted=True)
        self.generate_a_valid_schedule_of_each_type
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assert_no_scheduled_events
        repopulate_all_survey_scheduled_events(self.default_study)  # always test both arg setups
        self.assert_no_scheduled_events
    
    def test_permanently_retired_participant_gets_no_schedules(self):
        self.default_participant.update(permanently_retired=True)
        self.generate_a_valid_schedule_of_each_type
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assert_no_scheduled_events
        repopulate_all_survey_scheduled_events(self.default_study)  # always test both arg setups
        self.assert_no_scheduled_events
    
    def test_deleted_study_gets_no_schedules(self):
        self.default_study.update(deleted=True)
        self.using_default_participant()
        self.generate_a_valid_schedule_of_each_type
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assert_no_scheduled_events
        repopulate_all_survey_scheduled_events(self.default_study)
        self.assert_no_scheduled_events
    
    def test_manually_stopped_study_gets_no_schedules(self):
        self.default_study.update(manually_stopped=True)
        self.using_default_participant()
        self.generate_a_valid_schedule_of_each_type
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assert_no_scheduled_events
    
    def test_ended_study_gets_no_events(self):
        self.default_study.update(end_date=date.today() - timedelta(days=10))
        self.using_default_participant()
        self.generate_a_valid_schedule_of_each_type
        repopulate_all_survey_scheduled_events(self.default_study, self.default_participant)
        self.assert_no_scheduled_events
        repopulate_all_survey_scheduled_events(self.default_study)
        self.assert_no_scheduled_events
    
    def test_no_participants_in_study_generates_no_events(self):
        self.default_study.update(end_date=date.today() - timedelta(days=10))
        self.generate_a_valid_schedule_of_each_type
        repopulate_all_survey_scheduled_events(self.default_study)
        self.assert_no_scheduled_events
    
    # some other error modes...
    
    # Right, having a schedule is optional....
    def test_survey_has_no_schedules_relative(self):
        self.default_populated_intervention_date
        self.generate_relative_schedule(self.default_survey, self.default_intervention, days_after=0)
        repopulate_relative_survey_schedule_events(self.default_survey, self.default_participant)
        event = ScheduledEvent.obj_get()
        event.update(relative_schedule=None)
        repopulate_relative_survey_schedule_events(self.default_survey)
    
    def test_survey_has_no_schedules_weekly(self):
        self.generate_weekly_schedule(self.default_survey, day_of_week=0)
        repopulate_weekly_survey_schedule_events(self.default_survey, self.default_participant)
        event = ScheduledEvent.objects.first()
        event.update(weekly_schedule=None)
        repopulate_weekly_survey_schedule_events(self.default_survey)
    
    def test_survey_has_no_schedules_absolute(self):
        self.generate_absolute_schedule(timezone.now().date())
        repopulate_absolute_survey_schedule_events(self.default_survey, self.default_participant)
        event = ScheduledEvent.obj_get()
        event.update(absolute_schedule=None)
        repopulate_absolute_survey_schedule_events(self.default_survey)
    
    #
    ## The above are checks for the repopulate_all* functions, now we need to test some more
    ## complex behavior that connect to push notifications and when to send them.
    #
    
    def test_good_archive_event_with_absolute_schedule_helper_is_reasonable(self):
        d = MONDAY_JUNE_NOON_6_2022_EDT
        abs_sched = self.generate_absolute_schedule_from_datetime(self.default_survey, d)
        abs_archive = self.generate_archived_event_matching_absolute_schedule(abs_sched)
        # some real simple asserts that the archived event points at the correct items
        self.assertEqual(abs_archive.survey_archive.survey.id, self.default_survey.id)
        self.assertEqual(abs_archive.scheduled_time, MONDAY_JUNE_NOON_6_2022_EDT)
        self.assertIsNotNone(abs_archive.uuid)
    
    def test_good_archive_event_with_relative_schedule_helper_is_reasonable(self):
        rel_sched = self.generate_relative_schedule(
            self.default_survey, self.default_intervention, days_after=1, hours_after=1, minutes_after=1
        )
        
        # self.default_populated_intervention_date is defined as the current date, from which we
        # are manually generating a datetime object for the expected scheduled time.
        d = self.default_populated_intervention_date.date
        reference_time = datetime(
            year=d.year,
            month=d.month,
            day=d.day + 1,
            hour=1,
            minute=1,
            tzinfo=THE_ONE_TRUE_TIMEZONE
        )
        
        rel_archive = self.generate_archived_event_for_relative_schedule(
            rel_sched, self.default_participant
        )
        self.assertEqual(rel_archive.scheduled_time, reference_time)
        self.assertEqual(rel_archive.survey_archive.survey.id, self.default_survey.id)
        self.assertIsNotNone(rel_archive.uuid)
