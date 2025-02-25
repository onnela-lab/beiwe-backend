# trunk-ignore-all(ruff/B018,bandit/B101)
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from functools import wraps

import time_machine
from dateutil.tz import gettz
from django.utils import timezone

from constants.common_constants import DEV_TIME_FORMAT3
from constants.message_strings import MESSAGE_SEND_SUCCESS
from constants.user_constants import (ANDROID_API, IOS_API,
    IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION, IOS_APP_NO_RESENDS)
from database.common_models import TimestampedModel, UtilityModel
from database.schedule_models import (AbsoluteSchedule, ArchivedEvent, RelativeSchedule,
    ScheduledEvent, WeeklySchedule)
from database.survey_models import Survey
from database.system_models import GlobalSettings
from database.user_models_participant import (Participant, ParticipantFCMHistory,
    SurveyNotificationReport)
from libs.schedules import repopulate_all_survey_scheduled_events
from services.celery_push_notifications import get_surveys_and_schedules
from services.resend_push_notifications import (
    get_all_unconfirmed_notification_schedules_for_bundling, restore_scheduledevents_logic)
from tests.common import CommonTestCase


#
##  THIs TEST FILE IS WRETCHED
##  This feature was incredibly hard to get right, there are probably a lot of tests of duplicate
##  or seemingly-duplicate functionality.
#


ArchiveOrDt = ArchivedEvent|datetime
EventOrDt = ScheduledEvent|datetime


def weirdly_factored_setUp(self):
    # time_machine won't work right because self.setup() is called before the decorator runs we need
    # this bit factored out, and extra subclasses.
    self.START_OF_TEST_TIME = timezone.now()
    
    self.NOW_SORTA = NOW_SORTA = timezone.now().replace(second=0, microsecond=0).astimezone(gettz('America/New_York'))
    self.THE_BEGINNING_OF_TIME = NOW_SORTA - timedelta(days=1000)
    self.THE_PAST = NOW_SORTA - timedelta(days=5)
    self.THE_FUTURE = NOW_SORTA + timedelta(days=5)
    
    # get populated if you use the run_and_refresh_any_models helper
    self.BEFORE_RUN = None  # lol this weird factoring removes the type warning...
    self.AFTER_RUN = None
    
    self.default_absolute_schedule = self.generate_absolute_schedule_from_datetime(self.default_survey, self.THE_PAST)
    self.already_set_up_default_participant = False
    # these tests were all originally configured with a timer of 30 minutes.
    self.default_study.device_settings.update_only(resend_period_minutes=30)
    global_settings = GlobalSettings.singleton()
    global_settings.update(push_notification_resend_enabled=self.THE_BEGINNING_OF_TIME)


class The_Most_Meta_Class(CommonTestCase):
    """ This test runs the missing_notification_checkin_query across a variety of scenarios, similar
    to the way TestHeartbeatQuery works. """
    
    START_OF_TEST_TIME: datetime
    NOW_SORTA: datetime
    THE_BEGINNING_OF_TIME: datetime
    THE_PAST: datetime
    THE_FUTURE: datetime
    BEFORE_RUN: datetime
    AFTER_RUN: datetime
    default_absolute_schedule: AbsoluteSchedule
    already_set_up_default_participant: bool
    # the default study is in America/New_York, absolute time events get _reconstructed_ to be in
    # this timezone.  Also we need times with only minute precision.
    APP_VERSION = IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION
    
    def run_resend_logic_and_refresh_these_models(self, *args: UtilityModel):
        self.BEFORE_RUN = timezone.now()
        restore_scheduledevents_logic()
        self.AFTER_RUN = timezone.now()
        for model in args:
            model.refresh_from_db()
    
    @property
    def setup_participant_resend_push_basics(self):
        if hasattr(self, "already_set_up_default_participant") and self.already_set_up_default_participant:
            return
        self.set_default_participant_all_push_notification_features
        # we are not testing fcm token details in these tests.
        self.default_participant.update(last_upload=timezone.now())  # needs to be recently active
        self.already_set_up_default_participant = True
    
    @property
    def setup_participant_2(self) -> Participant:
        p2 = self.generate_participant(self.default_study)
        p2.update(last_version_name=self.APP_VERSION, os_type=IOS_API)
        self.set_participant_all_push_notification_features(p2)
        return p2
    
    # These were originally on a subclass, moved here for sharing
    
    def do_setup_for_resend_with_no_notification_report(
        self, participant: Participant|None = None
    ) -> tuple[ScheduledEvent, ArchivedEvent]:
        # single participant setups use the default participant
        if participant is None:
            participant = self.default_participant
            self.setup_participant_resend_push_basics
        
        # these aro stupid and obscure so I made some named functions to separate them
        sched_event = self._build_base_sched_event(participant)
        archive = self.build_base_archived_event(sched_event, participant)
        self._attach_archive_to_scheduled_event_as_if_sent(sched_event, archive)
        return sched_event, archive
    
    def do_setup_for_resend_with_unapplied_notification_report(self)\
          -> tuple[ScheduledEvent, ArchivedEvent, SurveyNotificationReport]:
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
    
    def build_base_archived_event(
        self, sched_event: ScheduledEvent, participant: Participant = None
    ) -> ArchivedEvent:
        archive = self.generate_archived_event_matching_absolute_schedule(
            sched_event.absolute_schedule, a_uuid=sched_event.uuid, participant=participant,
        )
        self.assertIsNotNone(archive.uuid)
        self.assertEqual(sched_event.uuid, archive.uuid)
        return archive
    
    def _attach_archive_to_scheduled_event_as_if_sent(self, sched_event: ScheduledEvent, archive: ArchivedEvent):
        # this value on archive is "reconstructed" to be in the study timezone, this check catches
        # if we change anything in our database test helpers like a timezone change.
        
        # FIXME: this test fails under unknown conditions.
        self.assertEqual(sched_event.scheduled_time, archive.scheduled_time)
        
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
    
    ## tools
    
    def assert_resend_logic_reenabled_schedule_correctly(self, sched_event: ScheduledEvent, archive: ArchivedEvent):
        ## this is the core test that resend logic found and reset a scheduled event.
        # The archive has been "touched", and confirmed_received is False
        
        try:
            self.assertNotEqual(
                self.THE_PAST, archive.last_updated, "\n\nArchive last_updated was not set during resend."
            )
            self.assert_touched_in_last_run(archive)
            self.assertFalse(archive.confirmed_received, "\n\nArchive confirmed_received was not set during resend.")
            
            # the scheduled event has deleted=False, was updated during logic
            self.assertFalse(sched_event.deleted, "\n\nScheduledEvent deleted was not set during resend.")
            self.assert_touched_in_last_run(sched_event)
            
            # all Notification reports should be applied (there may be None)
            for notification_report in SurveyNotificationReport.objects.all():
                self.assertEqual(notification_report.applied, True, "\n\nNotificationReport applied was not set during resend.")
            
            # the logic updates everything with the same last_updated time
            self.assert_last_updateds_equal(archive, sched_event)
            
            # in general an enabled schedule should not have no_resend True.
            self.assertFalse(sched_event.no_resend, "\n\nIn general an enabled schedule should not have no_resend True.")
        
        except AssertionError:
            print("\nNOW_SORTA:", self.NOW_SORTA.strftime(DEV_TIME_FORMAT3))
            print("THE_BEGINNING_OF_TIME:", self.THE_BEGINNING_OF_TIME.strftime(DEV_TIME_FORMAT3))
            print("THE_PAST:", self.THE_PAST.strftime(DEV_TIME_FORMAT3))
            print("THE_FUTURE:", self.THE_FUTURE.strftime(DEV_TIME_FORMAT3))
            print("BEFORE_RUN:", self.BEFORE_RUN.strftime(DEV_TIME_FORMAT3) if self.BEFORE_RUN else self.BEFORE_RUN)
            print("AFTER_RUN:", self.AFTER_RUN.strftime(DEV_TIME_FORMAT3) if self.AFTER_RUN else self.AFTER_RUN)
            print("APP_VERSION:", self.APP_VERSION, "\n")
            print("assertion error archive:")
            archive.pprint
            print("\nassertion error sched_event:")
            sched_event.pprint
            print()
            raise
    
    def assert_scheduled_event_not_sendable(self, sched_event: ScheduledEvent):
        self.assertEqual(sched_event.deleted, True, "scheduled event is not marked as deleted")
    
    def assert_touched_in_last_run(self, *models: TimestampedModel):
        messages = []
        for i, model in enumerate(models):
            name: str = model.__class__.__name__
            if not model.last_updated > self.BEFORE_RUN:
                messages.append(f"\n\n{name} [{i}] last_updated was not greater than self.BEFORE_RUN.\n" +
                               f"{model.last_updated.strftime(DEV_TIME_FORMAT3)}\n      !>\n{self.BEFORE_RUN.strftime(DEV_TIME_FORMAT3)}")
            if not model.last_updated < self.AFTER_RUN:
                messages.append(f"\n\n{name} [{i}] last_updated was not less than self.AFTER_RUN.\n" +
                                f"{model.last_updated.strftime(DEV_TIME_FORMAT3)}\n      !<\n{self.AFTER_RUN.strftime(DEV_TIME_FORMAT3)}")
        if messages:
            raise AssertionError("\n".join(messages))
    
    def assert_not_touched_in_last_run(self, *models: TimestampedModel):
        messages = []
        for i, model in enumerate(models):
            name: str = model.__class__.__name__
            if not model.last_updated < self.BEFORE_RUN:
                messages.append(f"\n\n{name} [{i}] last_updated >= self.BEFORE_RUN.\n" +
                                f"{model.last_updated.strftime(DEV_TIME_FORMAT3)}\n      !<\n{self.BEFORE_RUN.strftime(DEV_TIME_FORMAT3)}")
            if not model.last_updated < self.AFTER_RUN:
                messages.append(f"\n\n{name} [{i}] last_updated >= self.AFTER_RUN.\n" +
                                f"{model.last_updated.strftime(DEV_TIME_FORMAT3)}\n      !<\n{self.AFTER_RUN.strftime(DEV_TIME_FORMAT3)}")
        if messages:
            raise AssertionError("\n".join(messages))
    
    def assert_last_updateds_equal(self, *models: TimestampedModel):
        self._assert_last_updateds(1, *models)
    
    def assert_last_updateds_not_equal(self, *models: TimestampedModel):
        self._assert_last_updateds(len(models), *models)
    
    def _assert_last_updateds(self, match_count: int, *models: TimestampedModel):
        # check models passed in has same last_updated time as every other model, raise an assertion
        # error that states all missmatches.
        if len(models) < 2:
            raise ValueError("This function requires at least two models to compare.")
        if len(set(set(model.last_updated for model in models))) == match_count:
            return
        # assemble state of all models with their type and last updated time
        raise AssertionError("\n" + "\n".join(
            f"{model.last_updated.strftime(DEV_TIME_FORMAT3)} - {i} - {model.__class__.__name__}"
            for i, model in enumerate(models)
        ))
    
    def assert_counts(self, scheduled_event: int, archived_event: int, notification_report: int):
        self.assertEqual(ScheduledEvent.objects.count(), scheduled_event)
        self.assertEqual(ArchivedEvent.objects.count(), archived_event)
        self.assertEqual(SurveyNotificationReport.objects.count(), notification_report)
    
    def assert_last_updated_not_changed(
        self, archive_dt: ArchiveOrDt, sched_event_dt: EventOrDt, old_archive_dt: ArchiveOrDt,
        old_sched_event_dt: EventOrDt
    ):
        if not isinstance(archive_dt, datetime):
            archive_dt = archive_dt.last_updated
        if not isinstance(sched_event_dt, datetime):
            sched_event_dt = sched_event_dt.last_updated
        if not isinstance(old_archive_dt, datetime):
            old_archive_dt = old_archive_dt.last_updated
        if not isinstance(old_sched_event_dt, datetime):
            old_sched_event_dt = old_sched_event_dt.last_updated
        
        if archive_dt != old_archive_dt and sched_event_dt != old_sched_event_dt:
            raise AssertionError("neither archive nor scheduled event last_updated changed")
        
        self.assertEqual(archive_dt, old_archive_dt, "Only the archived events did not change.")
        self.assertEqual(sched_event_dt, old_sched_event_dt, "Only the scheduled events did not change.")


class The_Meta_Class(The_Most_Meta_Class):
    
    def setUp(self):
        super().setUp()
        weirdly_factored_setUp(self)


class TestResendLogicQuery(The_Meta_Class):
    
    ## meta
    
    def test_assert_touched_in_last_run(self):
        self.BEFORE_RUN = timezone.now()
        x = self.generate_fcm_token(self.default_participant)  # easy, has last_updated
        self.AFTER_RUN = timezone.now()
        # test passes
        self.assert_touched_in_last_run(x)
        self.assert_touched_in_last_run(x, x, x)
        # test fails on with after < last_update
        x = self.generate_fcm_token(self.default_participant)
        self.assertRaises(AssertionError, self.assert_touched_in_last_run, x)
        self.assertRaises(AssertionError, self.assert_touched_in_last_run, x, x)
        # test fails with after < last_updated and before > last_update
        self.BEFORE_RUN = timezone.now()  # no new object
        self.assertRaises(AssertionError, self.assert_touched_in_last_run, x)
        self.assertRaises(AssertionError, self.assert_touched_in_last_run, x, x)
        # test fails with just  before > last_update
        self.AFTER_RUN = timezone.now()  # no new object
        self.assertRaises(AssertionError, self.assert_touched_in_last_run, x)
        self.assertRaises(AssertionError, self.assert_touched_in_last_run, x, x)
    
    def test_assert_not_touched_in_last_run(self):
        x = self.generate_fcm_token(self.default_participant)
        t_start = x.last_updated
        self.BEFORE_RUN = timezone.now()
        t_middle = timezone.now()
        self.AFTER_RUN = timezone.now()
        t_end = timezone.now()
        self.assert_not_touched_in_last_run(x)  # everything is before last_updated
        self.assert_not_touched_in_last_run(x, x, x)
        x.force_update_only(last_updated=t_middle)  # last_updated is now in between
        self.assertRaises(AssertionError, self.assert_not_touched_in_last_run, x)
        self.assertRaises(AssertionError, self.assert_not_touched_in_last_run, x, x)
        x.force_update_only(last_updated=t_end)  # last_updated is now after both
        self.assertRaises(AssertionError, self.assert_not_touched_in_last_run, x)
        self.assertRaises(AssertionError, self.assert_not_touched_in_last_run, x, x)
        x = self.generate_fcm_token(self.default_participant)
        x.force_update_only(last_updated=self.BEFORE_RUN)
        self.AFTER_RUN = timezone.now()  # # before_run is now after last_updated but before after_run
        self.assertRaises(AssertionError, self.assert_not_touched_in_last_run, x)
        self.assertRaises(AssertionError, self.assert_not_touched_in_last_run, x, x)
    
    def test_assert_last_updateds(self):
        # this is a test of the test
        a = self.generate_fcm_token(self.default_participant)
        b = self.generate_fcm_token(self.default_participant)
        ParticipantFCMHistory.objects.all().update(last_updated=self.THE_PAST)
        a.refresh_from_db()
        b.refresh_from_db()
        self.assert_last_updateds_equal(a, b)
        self.assertRaises(AssertionError, self.assert_last_updateds_not_equal, a, b)
        ParticipantFCMHistory.fltr(pk=a.pk).update(last_updated=self.THE_FUTURE)
        a.refresh_from_db()
        self.assert_last_updateds_not_equal(a, b)
        self.assertRaises(AssertionError, self.assert_last_updateds_equal, a, b)
    
    ## one-participant tests
    
    # null tests
    
    def test_no_data(self):
        self.assert_counts(0, 0, 0)
        restore_scheduledevents_logic()
        self.assert_counts(0, 0, 0)
    
    def test_one_participant_nothing_else(self):
        self.using_default_participant()
        self.assert_counts(0, 0, 0)
        restore_scheduledevents_logic()
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
        self.default_participant.update_only(last_upload=self.THE_PAST - timedelta(days=8))
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        # this will find and operate on no database objects, confirm with last_updated on archive
        # because that is supposed to change if it is identified and used
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_last_updated_not_changed(
            archive, sched_event, old_archive_last_updated, old_sched_event_last_updated)
        self.assert_last_updateds_not_equal(archive, sched_event)
    
    def test_participant_deleted(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_participant.update_only(deleted=True)
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_last_updated_not_changed(
            archive, sched_event, old_archive_last_updated, old_sched_event_last_updated)
        self.assert_last_updateds_not_equal(archive, sched_event)
    
    def test_participant_retired(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_participant.update_only(permanently_retired=True)
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_last_updated_not_changed(
            archive, sched_event, old_archive_last_updated, old_sched_event_last_updated)
        self.assert_last_updateds_not_equal(archive, sched_event)
    
    def test_schedule_resend_disabled(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_study.device_settings.update(resend_period_minutes=0)
        sched_event.update_only(no_resend=True)
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_last_updated_not_changed(
            archive, sched_event, old_archive_last_updated, old_sched_event_last_updated)
        self.assert_last_updateds_not_equal(archive, sched_event)
    
    # archivedevent and scheduledevent behavior
    
    def test_archive_study_disabled_resend_of_0(self):
        self.assertEqual(self.default_study.device_settings.resend_period_minutes, 30)
        # recently updated archive should not result in resend
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_study.device_settings.update(resend_period_minutes=0)
        # this time is very in the past.
        ArchivedEvent.fltr(pk=archive.pk).update(last_updated=self.NOW_SORTA - timedelta(minutes=9999))
        old_archive_last_updated = archive.last_updated
        self.assertEqual(old_archive_last_updated, self.THE_PAST)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertNotEqual(archive.last_updated, old_archive_last_updated)
        self.assert_not_touched_in_last_run(archive, sched_event)
        self.assert_last_updateds_not_equal(archive, sched_event)
    
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
        self.assert_not_touched_in_last_run(archive, sched_event)
        self.assert_last_updateds_not_equal(archive, sched_event)
    
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
        self.assert_last_updateds_not_equal(archive, sched_event)
    
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
        self.assert_last_updateds_not_equal(archive, sched_event)
    
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
        self.assert_not_touched_in_last_run(sched_event, archive)
        self.assert_last_updateds_not_equal(archive, sched_event)
    
    def test_archive_with_no_uuid_and_scheduled_with_no_uuid(self):
        # identical to test_archive_with_no_uuid_and_scheduled_with_uuid but we clear both uuids)
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        ArchivedEvent.objects.filter(pk=archive.pk).update(uuid=None)
        ScheduledEvent.objects.filter(pk=sched_event.pk).update(uuid=None)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_last_updated_not_changed(
            archive, sched_event, old_archive_last_updated, old_sched_event_last_updated)
        self.assert_not_touched_in_last_run(sched_event, archive)
        self.assert_last_updateds_not_equal(archive, sched_event)
    
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
        self.assertIsNotNone(archive.uuid)
        self.assertIsNotNone(sched_event.uuid)
    
    def test_schedule_already_enabled(self):
        # should "work" in the sense that all the database objects will be modified and correct.
        # but I don't thin this state should ever happen.
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        old_archive_last_updated = archive.last_updated
        # print("original archive last updated", old_archive_last_updated)
        ScheduledEvent.objects.filter(pk=sched_event.pk).update(deleted=False)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
        self.assert_last_updateds_equal(archive, sched_event)
    
    def test_notification_report_correctly_blocks_a_followup_resend(self):
        sched_event, archive, report = self.do_setup_for_resend_with_unapplied_notification_report()
        self.assert_scheduled_event_not_sendable(sched_event)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive, report)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_not_touched_in_last_run(sched_event)
        self.assert_touched_in_last_run(archive, report)
        self.assert_last_updateds_equal(archive, report)
        self.assert_last_updateds_not_equal(archive, sched_event)
        self.assertTrue(report.applied)
        self.assertTrue(archive.confirmed_received)
        return sched_event, archive, report
    
    def test_doing_it_twice_in_succession_still_blocks(self):
        sched_event, archive, report = self.do_setup_for_resend_with_unapplied_notification_report()
        self.assert_scheduled_event_not_sendable(sched_event)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive, report)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_touched_in_last_run(archive, report)
        self.assert_not_touched_in_last_run(sched_event)
        self.assert_last_updateds_equal(archive, report)
        self.assertTrue(report.applied)
        self.assertTrue(archive.confirmed_received)
        # now we do it again
        old_before_run = self.BEFORE_RUN
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        self.run_resend_logic_and_refresh_these_models(sched_event, archive, report)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assertGreater(self.BEFORE_RUN, old_before_run)  # sanity check
        self.assert_last_updated_not_changed(
            archive, sched_event, old_archive_last_updated, old_sched_event_last_updated)
        self.assert_not_touched_in_last_run(sched_event, archive, report)
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
        self.assertEqual(self.default_study.device_settings.resend_period_minutes, 30)
        # should be duplicate test of minimum requirements
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        ArchivedEvent.objects.filter(pk=archive.pk).update(
            last_updated=self.NOW_SORTA - timedelta(minutes=31))
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
        
        # probably overkill or redundant
        self.assertGreater(archive.last_updated, old_archive_last_updated)
        self.assertGreater(sched_event.last_updated, old_sched_event_last_updated)
    
    def test_archive_last_updated_30_minutes_ago(self):
        self.assertEqual(self.default_study.device_settings.resend_period_minutes, 30)
        # should be duplicate test of minimum requirements
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        ArchivedEvent.objects.filter(pk=archive.pk).update(
            last_updated=self.NOW_SORTA - timedelta(minutes=30))
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
        
        # probably overkill or redundant
        self.assertGreater(archive.last_updated, old_archive_last_updated)
        self.assertGreater(sched_event.last_updated, old_sched_event_last_updated)
    
    def test_archive_last_updated_30_minutes_ago_messy_aka_fix_off_by_6_minutes(self):
        self.assertEqual(self.default_study.device_settings.resend_period_minutes, 30)
        # should be duplicate test of minimum requirements
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        ArchivedEvent.objects.filter(pk=archive.pk).update(
            last_updated=self.NOW_SORTA - timedelta(minutes=30))
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
        self.assert_touched_in_last_run(archive, report)
        self.assertTrue(report.applied)
        self.assertTrue(archive.confirmed_received)
        self.assertEqual(archive.uuid, report.notification_uuid)
        self.assert_last_updateds_equal(archive, report)
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
        self.assert_resend_logic_reenabled_schedule_correctly(p2_sched_event, p2_archive)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
    
    def test_2_participants_one_has_as_unapplied_report_one_has_no_report(self):
        sched_event, archive, report = self.do_setup_for_resend_with_unapplied_notification_report()
        p2 = self.setup_participant_2
        p2_sched_event, p2_archive = self.do_setup_for_resend_with_no_notification_report(p2)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive, report, p2_sched_event, p2_archive)
        # p1
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_touched_in_last_run(archive, report)
        self.assert_not_touched_in_last_run(sched_event)
        # p2
        self.assert_resend_logic_reenabled_schedule_correctly(p2_sched_event, p2_archive)
        # everything across participants should have the same last_updated time (except sched_event)
        self.assert_last_updateds_equal(archive, report, p2_archive, p2_sched_event)
        self.assert_last_updateds_not_equal(archive, sched_event)
    
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
        self.assert_not_touched_in_last_run(archive, report, sched_event)
        self.assert_last_updateds_equal(archive, report)
        # p2
        self.assert_resend_logic_reenabled_schedule_correctly(p2_sched_event, p2_archive)
        self.assert_touched_in_last_run(p2_archive, p2_sched_event)
        self.assert_last_updateds_equal(p2_archive, p2_sched_event)
    
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
        self.assert_last_updateds_equal(sched_event_1, sched_event_2)
        self.assert_last_updateds_equal(archive_1, archive_2)
    
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
        self.assert_last_updateds_equal(sched_event_1, sched_event_2)
        self.assert_last_updateds_equal(archive_1, archive_2)
    
    ## don't run before earliest resend time
    
    def test_correct_setup_before_earliest_resend_time_restriction_fails(self):
        global_settings = GlobalSettings.singleton()
        global_settings.update(push_notification_resend_enabled=self.THE_FUTURE)
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.assert_scheduled_event_not_sendable(sched_event)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_not_touched_in_last_run(archive, sched_event)
    
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


class TestGetUnconfirmedNotificationSchedules(The_Meta_Class):
    
    def test_one_participant_nothing_else(self):
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [])
    
    def test_base_case(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [sched_event])
    
    def test_ios_version_restriction_blocks(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.assertIsNotNone(sched_event.uuid)
        self.assertIsNotNone(archive.uuid)
        self.default_participant.update(os_type=IOS_API, last_version_name="2024.21")
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [])
    
    def test_ios_version_restriction_allows_equal(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_participant.update(os_type=IOS_API, last_version_name=self.APP_VERSION)
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [sched_event])
    
    def test_ios_version_restriction_allows_higher(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_participant.update(os_type=IOS_API, last_version_name="2024.29")
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [sched_event])
    
    def test_android_os_restriction_blocks(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_participant.update(os_type=ANDROID_API)
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [])
    
    def test_schedule_resend_feature_disabled_still_finds_resend(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.default_study.device_settings.update(resend_period_minutes=0)
        # yes, we want to bundle on studies with resends disabled. it is a a different and always desireable feature
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [sched_event])
    
    def test_created_too_early(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        archive.update(created_on=self.THE_PAST - timedelta(days=100000))
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [])
    
    def test_exclude_excludes(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant, [sched_event.pk]), [])
    
    def test_different_uuids_on_scheduled_event_and_archive(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        ScheduledEvent.objects.filter(pk=sched_event.pk).update(uuid=uuid.uuid4())
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [])
    
    def test_archive_with_uuid_and_scheduled_with_no_uuid(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        ScheduledEvent.objects.filter(pk=sched_event.pk).update(uuid=None)
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [])
    
    def test_archive_with_no_uuid_and_scheduled_with_uuid(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        ArchivedEvent.objects.filter(pk=archive.pk).update(uuid=None)
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [])
    
    def test_archive_with_no_uuid_and_scheduled_with_no_uuid(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        ArchivedEvent.objects.filter(pk=archive.pk).update(uuid=None)
        ScheduledEvent.objects.filter(pk=sched_event.pk).update(uuid=None)
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [])
    
    def test_archive_with_uuid_and_scheduled_actually_deleted(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        ScheduledEvent.objects.all().delete()
        self.assertEqual(ArchivedEvent.objects.count(), 1)
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [])
    
    def test_not_resendable_with_scheduled_event_that_has_no_schedule(self):
        sendable_sched_event = self._build_base_sched_event(self.default_participant)
        sendable_sched_event.update(absolute_schedule=None)  # its an absolute schedule
        self.assertIsNone(sendable_sched_event.absolute_schedule)
        self.assertFalse(sendable_sched_event.deleted)
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [])
    
    def test_archive_last_updated_more_than_30_minutes_ago(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        archive.update(last_updated=self.NOW_SORTA - timedelta(minutes=31))
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [sched_event])
    
    def test_archive_last_updated_more_than_just_now_minutes_ago(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        archive.update(last_updated=timezone.now())
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [sched_event])
    
    def test_archive_last_updated_way_in_the_past(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        archive.update(last_updated=timezone.now() - timedelta(days=999))
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [sched_event])
    
    def test_archive_last_updated_in_the_future(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        archive.update(last_updated=timezone.now() + timedelta(hours=999))
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [sched_event])
    
    def test_schedule_unsent_in_past_no_archive(self):
        sched_event = self._build_base_sched_event(self.default_participant)
        sched_event.update(scheduled_time=timezone.now() - timedelta(hours=999))  # redundant
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [])
    
    def test_schedule_unsent_in_future_no_archive(self):
        sched_event = self._build_base_sched_event(self.default_participant)
        sched_event.update(scheduled_time=timezone.now() + timedelta(hours=999))
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [])
    
    def test_2_participants_both_with_resends_only_gets_correct_participant(self):
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        p2 = self.setup_participant_2
        p2_sched_event, p2_archive = self.do_setup_for_resend_with_no_notification_report(p2)
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant), [sched_event])
        self.assertEqual(get_all_unconfirmed_notification_schedules_for_bundling(p2), [p2_sched_event])
    
    def test_multiple_archive_and_scheduled_events_on_one_survey(self):
        sched_event_1, archive_1 = self.do_setup_for_resend_with_no_notification_report()
        sched_event_2, archive_2 = self.do_setup_for_resend_with_no_notification_report()
        # ack sorting may not be deterministic
        scheds = get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant)
        scheds.sort(key=lambda x: x.id)
        scheds_compare = [sched_event_1, sched_event_2]
        scheds_compare.sort(key=lambda x: x.id)
        self.assertEqual(scheds, scheds_compare)
    
    def test_multiple_archive_and_scheduled_events_on_two_surveys(self):
        sched_event_1, archive_1 = self.do_setup_for_resend_with_no_notification_report()
        
        another_survey = self.generate_survey(self.default_study, Survey.TRACKING_SURVEY)
        abs_sched_2 = self.generate_absolute_schedule_from_datetime(another_survey, self.THE_PAST)
        sched_event_2 = self.generate_scheduled_event(
            another_survey, self.default_participant, abs_sched_2, self.THE_PAST,
        )
        archive = self.build_base_archived_event(sched_event_2, self.default_participant)
        self._attach_archive_to_scheduled_event_as_if_sent(sched_event_2, archive)
        
        # ack sorting may not be deterministic
        scheds = get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant)
        scheds.sort(key=lambda x: x.id)
        scheds_compare = [sched_event_1, sched_event_2]
        scheds_compare.sort(key=lambda x: x.id)
        self.assertEqual(scheds, scheds_compare)
    
    ## tests of one schedule with multiple archives
    def test_one_schedule_multiple_archives_no_resends_or_confirmed(self):
        sched_event, archive_1 = self.do_setup_for_resend_with_no_notification_report()
        archive_2 = self.build_base_archived_event(sched_event, self.default_participant)
        self.assertEqual(archive_1.uuid, sched_event.uuid)
        self.assertEqual(archive_1.uuid, archive_2.uuid)
        self.assertEqual(archive_1.was_resend, False)
        self.assertEqual(archive_2.was_resend, False)  # technically should not be possible
        self.assertEqual(archive_1.confirmed_received, False)
        self.assertEqual(archive_2.confirmed_received, False)
        scheds = get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant)
        scheds_compare = [sched_event]
        self.assertEqual(scheds, scheds_compare)
    
    def test_one_schedule_multiple_archives_1_send_1_resend_no_confirmed(self):
        sched_event, archive_1 = self.do_setup_for_resend_with_no_notification_report()
        archive_2 = self.build_base_archived_event(sched_event, self.default_participant)
        archive_2.update(was_resend=True)
        scheds = get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant)
        scheds_compare = [sched_event]
        self.assertEqual(scheds, scheds_compare)
    
    def test_one_schedule_multiple_archives_1_send_1_resend_no_confirmed_real_times_1(self):
        sched_event, archive_1 = self.do_setup_for_resend_with_no_notification_report()
        self.default_study.device_settings.update(resend_period_minutes=60)
        archive_2 = self.build_base_archived_event(sched_event, self.default_participant)
        now = timezone.now()
        ago_30 = now - timedelta(minutes=30)
        ago_90 = now - timedelta(minutes=90)
        # invalid state: archive 1 is the resend
        archive_1.force_update_only(was_resend=True, last_updated=ago_90)
        archive_2.force_update_only(was_resend=False, last_updated=ago_30)
        scheds = get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant)
        scheds_compare = [sched_event]
        self.assertEqual(scheds, scheds_compare)
    
    def test_one_schedule_multiple_archives_one_resend_both_confirmed(self):
        sched_event, archive_1 = self.do_setup_for_resend_with_no_notification_report()
        now = timezone.now()
        scheds_compare = [sched_event]
        ago_30 = now - timedelta(minutes=30)
        ago_90 = now - timedelta(minutes=90)
        
        # single schedule before resend period is over - still trigger:
        archive_1.force_update_only(last_updated=ago_30)
        scheds = get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant)
        self.assertEqual(scheds, scheds_compare)
        # resend timer activated, still trigger:
        archive_1.force_update_only(last_updated=ago_90)
        scheds = get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant)
        self.assertEqual(scheds, scheds_compare)
        # second archive, resend timer inactive, still trigger:
        archive_2 = self.build_base_archived_event(sched_event, self.default_participant)
        archive_2.force_update_only(was_resend=True, last_updated=ago_30)
        scheds = get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant)
        self.assertEqual(scheds, scheds_compare)
        
        # 30 minutes pass, still trigger:
        with time_machine.travel(now + timedelta(minutes=30)):
            scheds = get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant)
            self.assertEqual(scheds, scheds_compare)
        # 60 minutes pass, still trigger:
        with time_machine.travel(now + timedelta(minutes=60)):
            scheds = get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant)
            self.assertEqual(scheds, scheds_compare)
    
    def test_3_on_out_recent(self):
        sched_event, archive_1 = self.do_setup_for_resend_with_no_notification_report()
        self.default_study.device_settings.update(resend_period_minutes=60)
        archive_2 = self.build_base_archived_event(sched_event, self.default_participant)
        archive_3 = self.build_base_archived_event(sched_event, self.default_participant)
        now = timezone.now()
        ago_12 = now - timedelta(minutes=12)
        ago_30 = now - timedelta(minutes=30)
        ago_90 = now - timedelta(minutes=90)
        archive_1.force_update_only(was_resend=False, last_updated=ago_90)
        archive_2.force_update_only(was_resend=True, last_updated=ago_30)  # this is not within the
        archive_3.force_update_only(was_resend=True, last_updated=ago_12)
        scheds = get_all_unconfirmed_notification_schedules_for_bundling(self.default_participant)
        scheds_compare = [sched_event]
        self.assertEqual(scheds, scheds_compare)
    
    
    # ok I have to do the test for:
    # - the sched_event needs to be the same sched_event
    # - archive A last_updated 30 minutes+ ago  and  archive B last_updated 10 minutes ago
    # - study period 30 minutes
    # - the result before fixing it should be a for the 30 to cause to get returned.
    # After fix - neither is returned
    # now = timezone.now()
    # archive_2.update(last_updated=now - timedelta(minutes=10))


#
## even more setup for more duplicate tests
#


UP_BEFORE_ARCHIVE_CREATED = "participant__before_archive_created"
UP_AFTER_ARCHIVE_CREATED = "participant__after_archive_created"
P_NEVER_ENABLE = "participant__never_enable"

VALID_OPTIONS_PARTICIPANT_UPGRADE = {
    UP_BEFORE_ARCHIVE_CREATED,
    # too late:
    UP_AFTER_ARCHIVE_CREATED,
    P_NEVER_ENABLE,
}
P_SHOULD_NOT_RESEND = {
    P_NEVER_ENABLE,
    UP_AFTER_ARCHIVE_CREATED,
}
P_SHOULD_RESEND = {
    UP_BEFORE_ARCHIVE_CREATED,
}


RESEND_BEFORE_ARCHIVE_CREATED = "resend__before_archived_events"
RESEND_AFTER_ARCHIVE_CREATED = "resend__after_archived_events_created"
RESEND_LEAVE_IT_NONE = "resend__leave_it_none"

VALID_OPTIONS_SET_RESEND = {
    RESEND_BEFORE_ARCHIVE_CREATED,  # only this one is should resend
    RESEND_AFTER_ARCHIVE_CREATED,
    RESEND_LEAVE_IT_NONE,
}
RESEND_SHOULD_RESEND = {
    RESEND_BEFORE_ARCHIVE_CREATED,
}
RESEND_SHOULD_NOT_RESEND = {
    RESEND_AFTER_ARCHIVE_CREATED,
    RESEND_LEAVE_IT_NONE,
}


# TODO: are we excluding archives lacking uuids? correctly with all the time orderings I have tested?


class TestResendBlockers(CommonTestCase):
    # We can't have *all historical push notifications* trigger when a participant updates their
    # app version on a server where resend is already enabled. THAT WOULD BE REAL BAD.
    
    ## meta test
    def test_configuration(self):
        self.assertSetEqual(
            P_SHOULD_RESEND.union(P_SHOULD_NOT_RESEND),
            VALID_OPTIONS_PARTICIPANT_UPGRADE,
            "All items for particiants must be present in the two sets.",
        )
        self.assertSetEqual(
            RESEND_SHOULD_RESEND.union(RESEND_SHOULD_NOT_RESEND),
            VALID_OPTIONS_SET_RESEND,
             "All items for resends must be present in the two sets.",
        )
    
    ## test variants
    def test_R_before_archive_created____P_before_archive_created____uuids(self):
        self._the_test(RESEND_BEFORE_ARCHIVE_CREATED, UP_BEFORE_ARCHIVE_CREATED, uuids=True)
    
    def test_R_before_archive_created____P_before_archive_created____no_uuids(self):
        self._the_test(RESEND_BEFORE_ARCHIVE_CREATED, UP_BEFORE_ARCHIVE_CREATED, uuids=False)
    
    def test_R_before_archive_created____P_after_archive_created____uuids(self):
        self._the_test(RESEND_BEFORE_ARCHIVE_CREATED, UP_AFTER_ARCHIVE_CREATED, uuids=True)
    
    def test_R_before_archive_created____P_after_archive_created____no_uuids(self):
        self._the_test(RESEND_BEFORE_ARCHIVE_CREATED, UP_AFTER_ARCHIVE_CREATED, uuids=False)
    
    def test_R_after_archive_created____P_before_archive_created____uuids(self):
        self._the_test(RESEND_AFTER_ARCHIVE_CREATED, UP_BEFORE_ARCHIVE_CREATED, uuids=True)
    
    def test_R_after_archive_created____P_before_archive_created____no_uuids(self):
        self._the_test(RESEND_AFTER_ARCHIVE_CREATED, UP_BEFORE_ARCHIVE_CREATED, uuids=False)
    
    def test_R_after_archive_created____P_after_archive_created____uuids(self):
        self._the_test(RESEND_AFTER_ARCHIVE_CREATED, UP_AFTER_ARCHIVE_CREATED, uuids=True)
    
    def test_R_after_archive_created____P_after_archive_created____no_uuids(self):
        self._the_test(RESEND_AFTER_ARCHIVE_CREATED, UP_AFTER_ARCHIVE_CREATED, uuids=False)
    
    def test_R_leave_it_none____P_before_archive_created____uuids(self):
        self._the_test(RESEND_LEAVE_IT_NONE, UP_BEFORE_ARCHIVE_CREATED, uuids=True)
    
    def test_R_leave_it_none____P_before_archive_created____no_uuids(self):
        self._the_test(RESEND_LEAVE_IT_NONE, UP_BEFORE_ARCHIVE_CREATED, uuids=False)
    
    def test_R_leave_it_none____P_after_archive_created____uuids(self):
        self._the_test(RESEND_LEAVE_IT_NONE, UP_AFTER_ARCHIVE_CREATED, uuids=True)
    
    def test_R_leave_it_none____P_after_archive_created____no_uuids(self):
        self._the_test(RESEND_LEAVE_IT_NONE, UP_AFTER_ARCHIVE_CREATED, uuids=False)
    
    ## test readability:
    def global_settings_resend_enabled_to_now(self):
        GlobalSettings.singleton().update(push_notification_resend_enabled=timezone.now())
    
    def set_default_participant_version_no_resends(self):
        self.default_participant.update(last_upload=timezone.now(), last_version_name=IOS_APP_NO_RESENDS)
    
    def set_default_participant_version_yes_resends(self):
        self.default_participant.update(last_version_name=IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION)
    
    ## The Test But This Time For Real
    def _the_test(
        self,
        set_resend: str,
        participant_upgrade: str,
        uuids: bool,
        archive_minutes_past: bool = False
    ):
        self._validate_params(set_resend, participant_upgrade)
        
        self.set_default_participant_all_push_notification_features
        self.set_default_participant_version_no_resends()
        
        if set_resend == RESEND_BEFORE_ARCHIVE_CREATED:
            self.global_settings_resend_enabled_to_now()
        if participant_upgrade == UP_BEFORE_ARCHIVE_CREATED:
            self.set_default_participant_version_yes_resends()
        
        # use the REAL mechanism for creating two schedules and scheduled events, 60 seconds apart.
        AbsoluteSchedule.configure_absolute_schedules(
            [(2020, 1, 1, 0), (2020, 1, 1, 60)], self.default_survey,  # deep past
        )
        repopulate_all_survey_scheduled_events(self.default_study)
        
        if participant_upgrade == P_NEVER_ENABLE:
            self.assertEqual(self.default_participant.last_version_name, IOS_APP_NO_RESENDS)
        
        # get the two schedules, create a successful archive of the first, force last_updated to be
        # old.  sched_with_arch currently not sendable, sched_no_arch sendable
        sched_with_arch, sched_no_arch = ScheduledEvent.objects.order_by("scheduled_time")
        sched_with_arch.archive(self.default_participant, MESSAGE_SEND_SUCCESS)
        ArchivedEvent.objects.update(last_updated=timezone.now() - timedelta(days=1))
        if not uuids:
            ArchivedEvent.objects.update(uuid=None)
        sched_with_arch.refresh_from_db()
        sched_no_arch.refresh_from_db()
        
        # toggles....
        if participant_upgrade == UP_AFTER_ARCHIVE_CREATED:
            self.set_default_participant_version_yes_resends()
        if set_resend == RESEND_AFTER_ARCHIVE_CREATED:
            self.global_settings_resend_enabled_to_now()
        
        now = timezone.now()  # value is after created_on of the archive
        fcm_token = self.default_participant.get_valid_fcm_token().token
        
        # Will be 1 schedule, 1 survey, 1 participant before running resend logic
        self.assert_critical_query_finds_only_this_schedule(now, fcm_token, sched_no_arch.pk)
        restore_scheduledevents_logic()
        
        # THE PERMUTATIONS
        if participant_upgrade in P_SHOULD_RESEND and set_resend in RESEND_SHOULD_RESEND:
            # but only for archives with uuids (probably not real but should be handled, other
            # scenarios exist where we clear the uuid but I also want to remove that....)
            if uuids:
                self.assert_find_both_schedules(now, fcm_token, sched_with_arch, sched_no_arch)
            else:
                self.assert_critical_query_finds_only_this_schedule(now, fcm_token, sched_no_arch.pk)
        elif participant_upgrade in P_SHOULD_NOT_RESEND or set_resend in RESEND_SHOULD_NOT_RESEND:
            self.assert_critical_query_finds_only_this_schedule(now, fcm_token, sched_no_arch.pk)
        else:
            raise Exception("something about the test configuration is wrong, SAW-RYYY")
        
        # meta, validate the earliest time to save our future selves if we update the test...
        # it has to be non-None and in the past, this value is not allowed be set to the future.
        resend_enabled = GlobalSettings.singleton().push_notification_resend_enabled
        if set_resend == RESEND_LEAVE_IT_NONE:
            self.assertIsNone(resend_enabled)
        else:
            self.assertLess(resend_enabled, timezone.now())
    
    ## THE ASSERTIONS
    def assert_critical_query_finds_only_this_schedule(self, now: datetime, fcm_token: str, schedule_pk: int):
        surveys_by_fcm, schedules_by_fcm, participants_by_fcm = get_surveys_and_schedules(now)
        # from pprint import pprint
        # print("fcm_token:", fcm_token)
        # print("surveys_by_fcm:")
        # pprint(surveys_by_fcm)
        # print("schedules_by_fcm:")
        # pprint(schedules_by_fcm)
        # print("participants_by_fcm:")
        # pprint(participants_by_fcm)
        # one of each
        self.assertEqual(len(surveys_by_fcm[fcm_token]), 1)
        self.assertEqual(len(schedules_by_fcm[fcm_token]), 1)
        self.assertEqual(len(participants_by_fcm), 1)
        # should find B
        should_be_pk_b = schedules_by_fcm[fcm_token][0]
        self.assertEqual(schedule_pk, should_be_pk_b)
    
    def assert_find_both_schedules(self, now: datetime, fcm_token: str, a: ScheduledEvent, b: ScheduledEvent):
        surveys_by_fcm, schedules_by_fcm, participants_by_fcm = get_surveys_and_schedules(now)
        # one of each
        self.assertEqual(len(surveys_by_fcm[fcm_token]), 2)  # not deduplicated yet
        self.assertEqual(len(schedules_by_fcm[fcm_token]), 2)
        self.assertEqual(len(participants_by_fcm), 1)
        # should find both
        self.assertIn(a.pk, schedules_by_fcm[fcm_token])
        self.assertIn(b.pk, schedules_by_fcm[fcm_token])
    
    def _validate_params(self, set_resend: str, participant_upgrade: str):
        # after running migrations, the push_notification_resend_enabled should be None.
        self.assertIsNone(
            GlobalSettings.singleton().push_notification_resend_enabled,
            "\n\nYou need to rebuild your test database."
        )
        
        if "resend__" not in set_resend or "participant__" in set_resend:
            raise ValueError(f"bad set_resend: '{set_resend}'")
        
        if "participant__" not in participant_upgrade or "resend__" in participant_upgrade:
            raise ValueError(f"bad participant_upgrade: '{participant_upgrade}'")
        
        if set_resend not in VALID_OPTIONS_SET_RESEND:
            raise ValueError("set_resend must be a specific value.")
        
        if participant_upgrade not in VALID_OPTIONS_PARTICIPANT_UPGRADE:
            raise ValueError("participant_upgrade must be a specific value.")
    
    
class TestResendTiming(The_Most_Meta_Class):
    def _test_setup(self):
        self.setup_participant_resend_push_basics
        self.default_study.device_settings.update(resend_period_minutes=30)  # force...
        GlobalSettings().update(push_notification_resend_enabled=timezone.now())
    def refresh(self, *models):
        for m in models:
            m.refresh_from_db()
    def create_extra_schedules(self):
        AbsoluteSchedule.configure_absolute_schedules(
            [(2020, 1, 1, 60), (2021, 1, 1, 60)],
            self.default_survey,
        )
        repopulate_all_survey_scheduled_events(self.default_study)
    
    ### The More Basic logic tests
    
    def test_resend_timing_1_sent_too_recent(self):
        self._test_resend_timing([timezone.now() - timedelta(minutes=2)], reenabled=False)
    
    def test_resend_timing_1_sent_30_minutes_ago(self):
        self._test_resend_timing([timezone.now() - timedelta(minutes=30)], reenabled=True)
    
    def test_resend_timing_1_sent_enough_minutes_ago(self):
        self._test_resend_timing(
            [timezone.now() - timedelta(minutes=30)],
            reenabled=True,
        )
    
    def test_resend_timing_2_sent_30_minutes_successive(self):
        self._test_resend_timing(
            [timezone.now() - timedelta(minutes=60), timezone.now() - timedelta(minutes=30)],
            reenabled=True,
        )
    
    def test_resend_timing_2_sent_30_minutes_successive_1_but_minus_10_minutes(self):
        self._test_resend_timing(
            [timezone.now() - timedelta(minutes=50), timezone.now() - timedelta(minutes=20)],
            reenabled=False,
        )
    
    def _test_resend_timing(self, archive_times: list[datetime], reenabled: bool):
        self._test_setup()
        self._make_one_deep_past_absolute_schedule()
        
        # get the two schedules, create a successful archive of the first, force last_updated to be
        # old.  sched_with_arch currently not sendable, sched_no_arch sendable
        event = ScheduledEvent.objects.order_by("scheduled_time")[0]
        event.update_only(deleted=True)
        archives = self._make_archives_from_times(event, *archive_times)
        restore_scheduledevents_logic()  # run code!
        
        if reenabled:
            self.assert_resend_reenabled(event, archives, archive_times)
        else:
            self.assert_scheduled_event_not_sendable(event)
    
    ### tests for bundling?
    def test_resend_timing_2_schedules_too_recent(self):
        self._test_setup()
        self._make_two_deep_past_absolute_schedules()
        t1 = timezone.now() - timedelta(minutes=2)
        t2 = timezone.now() - timedelta(minutes=2)
        ScheduledEvent.objects.update(deleted=True)
        event1, event2 = ScheduledEvent.objects.order_by("scheduled_time")
        a1 = self._make_archives_from_times(event1, t1)
        a2 = self._make_archives_from_times(event2, t2)
        restore_scheduledevents_logic()
        # self.refresh(event1, event2, a1, a2)
        self.assert_resend_inactive(event1, a1, [t1])
        self.assert_resend_inactive(event2, a2, [t2])
    
    def test_resend_timing_2_schedules_sent_30_minutes_ago(self):
        self._test_setup()
        self._make_two_deep_past_absolute_schedules()
        ScheduledEvent.objects.update(deleted=True)
        event1, event2 = ScheduledEvent.objects.order_by("scheduled_time")
        t1 = timezone.now() - timedelta(minutes=30)
        t2 = timezone.now() - timedelta(minutes=30)
        a1 = self._make_archives_from_times(event1, t1)
        a2 = self._make_archives_from_times(event2, t2)
        restore_scheduledevents_logic()
        # event1.refresh_from_db()
        # event2.refresh_from_db()
        self.assert_resend_reenabled(event1, a1, [t1])
        self.assert_resend_reenabled(event2, a2, [t2])
    
    ### Absolute Schedules
    def _make_one_deep_past_absolute_schedule(self):
        self._make_schedules_and_archives_raw((2020, 1, 1, 60))
    
    def _make_two_deep_past_absolute_schedules(self):
        self._make_schedules_and_archives_raw((2020, 1, 1, 60), (2021, 1, 1, 60))
    
    def _make_schedules_and_archives_raw(self, *times: tuple[int, int, int, int]):
        # form is [(2020, 1, 1, 60)] as Year, Month, Day, SecondsIntoDay
        AbsoluteSchedule.configure_absolute_schedules(times, self.default_survey)
        repopulate_all_survey_scheduled_events(self.default_study)
    
    ### Archives
    def _make_archives_from_times(self, event: ScheduledEvent, *times: datetime):
        archives: list[ArchivedEvent] = []
        for t in times:
            event.archive(self.default_participant, MESSAGE_SEND_SUCCESS)  # sets deleted=True on event
            archives.append(ArchivedEvent.objects.latest("created_on"))
            archives[-1].force_update_only(last_updated=t)
        return archives
    
    ### Assertions
    def assert_resend_reenabled(self, sched_event: ScheduledEvent, archives: list[ArchivedEvent], times: list[datetime]):
        self.assertEqual(len(archives), len(times))
        self.refresh(sched_event, *archives)
        self.assertFalse(sched_event.no_resend, "\n\nScheduledEvent should not have no_resend True.")
        self.assertFalse(sched_event.deleted, "\n\nScheduledEvent.deleted was not set during resend.")
        
        for i, (archive, t) in enumerate(zip(archives, times)):
            try:
                self.assertNotEqual(t, archive.last_updated, f"\n\nArchive [{i}] last_updated was not set during resend.\n")
                self.assert_last_updateds_equal(archive, sched_event)
            except AssertionError:
                print("assertion error archives:")
                for a in archives:
                    if a is archive: print(">>>>>>>>>>>")
                    a.pprint
                    if a is archive: print("<<<<<<<<<<<")
                print("\nassertion error sched_event:")
                sched_event.pprint
                print()
                raise
    
    def assert_resend_inactive(self, sched_event: ScheduledEvent, archives: list[ArchivedEvent], times: list[datetime]):
        self.assertEqual(len(archives), len(times))
        self.refresh(sched_event, *archives)
        self.assertFalse(sched_event.no_resend, "\n\nthese tests are not for disabling resends.")
        self.assertTrue(sched_event.deleted, "\n\nScheduledEvent.deleted was not set during resend.")                
        
        for archive, t in zip(archives, times):
            archive.refresh_from_db()
            
            try:
                self.assertEqual(t, archive.last_updated, "\n\nArchive last_updated was set during resend.\n")
                self.assert_last_updateds_not_equal(archive, sched_event)
            except AssertionError:
                print("assertion error archives:")
                for a in archives:
                    if a is archive: print(">>>>>>>>>>>")
                    a.pprint
                    if a is archive: print("<<<<<<<<<<<")
                print("\nassertion error sched_event:")
                sched_event.pprint
                print()
                raise
        

class TestTheOnesWithTimeTravel(The_Most_Meta_Class):
    
    # @staticmethod
    class time_travel:
        """ Used to wrap a test method and call the time-travel sensitive setup code. """
        def __init__(self, t_str: str, *args, **kwargs):
            t_str_lower = t_str.lower()
            if "utc" in t_str_lower or "est" in t_str_lower or "edt" in t_str_lower:
                raise ValueError("setting the datetime library to use a timezone is always wrong.")
            self.t_str = t_str
            self.args = args
            self.kwargs = kwargs
        
        def __call__(self, func):
            @wraps(func)
            def wrapper(func_self, *func_args, **func_kwargs):
                with time_machine.travel(self.t_str, tick=True, *self.args, **self.kwargs):
                    weirdly_factored_setUp(func_self)
                    return func(func_self, *func_args, **func_kwargs)
            return wrapper
    
    @time_travel("2024-01-01 13:30:00")
    def test_30_minutes_out_both_on_a_boundary_works(self):
        self._assert_timers_trigger(30, 30)
    @time_travel("2024-01-01 13:30:00")
    def test_29_minutes_out_both_on_a_boundary_works(self):
        self._assert_timers_trigger(29, 29)
    @time_travel("2024-01-01 13:30:00")
    def test_9_min_after_boundary_30_min_timeer_29_min_since(self):
        self._assert_timers_trigger(30, 29)
    @time_travel("2024-01-01 13:30:00")
    def test_9_min_after_boundary_30_min_timeer_28_min_since(self):
        self._assert_timers_trigger(30, 28)
    @time_travel("2024-01-01 13:30:00")
    def test_9_min_after_boundary_30_min_timeer_27_min_since(self):
        self._assert_timers_trigger(30, 28)
    @time_travel("2024-01-01 13:30:00")
    def test_9_min_after_boundary_30_min_timeer_26_min_since(self):
        self._assert_timers_trigger(30, 26)
    @time_travel("2024-01-01 13:30:00")
    def test_9_min_after_boundary_30_min_timeer_25_min_since(self):
        self._assert_timers_trigger(30, 25)
    @time_travel("2024-01-01 13:30:00")
    def test_9_min_after_boundary_30_min_timeer_24_min_since(self):
        self.assert_timers_dont_trigger(30, 24)
    @time_travel("2024-01-01 13:31:00")
    def test_1_min_after_boundary_30_min_timeer_29_min_since(self):
        self._assert_timers_trigger(30, 29)
    @time_travel("2024-01-01 13:31:00")
    def test_1_min_after_boundary_30_min_timeer_28_min_since(self):
        self._assert_timers_trigger(30, 28)
    @time_travel("2024-01-01 13:31:00")
    def test_1_min_after_boundary_30_min_timeer_27_min_since(self):
        self._assert_timers_trigger(30, 27)
    @time_travel("2024-01-01 13:31:00")
    def test_1_min_after_boundary_30_min_timeer_26_min_since(self):
        self._assert_timers_trigger(30, 26)
    @time_travel("2024-01-01 13:31:00")
    def test_1_min_after_boundary_30_min_timeer_25_min_since(self):
        self.assert_timers_dont_trigger(30, 25)
    @time_travel("2024-01-01 13:31:00")
    def test_1_min_after_boundary_30_min_timeer_24_min_since(self):
        self.assert_timers_dont_trigger(30, 24)
    
    # The above is sufficient to test that it squeezes down within the 6 minute bracket.
    
    def assert_timers_dont_trigger(self, period: int, sent_ago: int):
        self.default_study.device_settings.update(resend_period_minutes=period)
        # recently updated archive should not result in resend
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        # 29 minutes will occasionally trigger due to if the self.NOW_SORTA is right at the end of a
        # minute and that minute has passed by the time this test runs. I think.
        ArchivedEvent.objects.all().update(last_updated=self.NOW_SORTA - timedelta(minutes=sent_ago))
        old_archive_last_updated = archive.last_updated
        self.assertEqual(old_archive_last_updated, self.THE_PAST)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assertNotEqual(archive.last_updated, old_archive_last_updated)
        self.assert_scheduled_event_not_sendable(sched_event)
        self.assert_not_touched_in_last_run(archive, sched_event)
        self.assert_last_updateds_not_equal(archive, sched_event)
    
    def _assert_timers_trigger(self, period: int, sent_ago: int):
        self.default_study.device_settings.update(resend_period_minutes=period)
        # recently updated archive should result in resend
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        ArchivedEvent.fltr(pk=archive.pk).update(last_updated=self.NOW_SORTA - timedelta(minutes=sent_ago))
        old_archive_last_updated = archive.last_updated
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
    
    # literally had a bug where the code that was intended to fix 6 minutes off bugs applied to the
    # wrong datetime object (calculated the survey timer value), result was that the test failed
    # when run during the final 5 minutes of the period after it should have gone off, but not the
    # 0th minute.
    
    # this may have become a duplicate of a previous test above....
    
    @time_travel("2025-2-18 6:24pm")
    def test_archive_last_updated_30_minutes_ago_bug_6_24(self):
        self._test_test_archive_last_updated_30_minutes_ago()
    
    @time_travel("2025-2-18 6:25pm")
    def test_archive_last_updated_30_minutes_ago_bug_6_25(self):
        self._test_test_archive_last_updated_30_minutes_ago()
    
    @time_travel("2025-2-18 6:26pm")
    def test_archive_last_updated_30_minutes_ago_bug_6_26(self):
        self._test_test_archive_last_updated_30_minutes_ago()
    
    @time_travel("2025-2-18 6:27pm")
    def test_archive_last_updated_30_minutes_ago_bug_6_27(self):
        self._test_test_archive_last_updated_30_minutes_ago()
    
    @time_travel("2025-2-18 6:28pm")
    def test_archive_last_updated_30_minutes_ago_bug_6_28(self):
        self._test_test_archive_last_updated_30_minutes_ago()
    
    @time_travel("2025-2-18 6:29pm")
    def test_archive_last_updated_30_minutes_ago_bug_6_29(self):
        self._test_test_archive_last_updated_30_minutes_ago()
    
    @time_travel("2025-2-18 6:30:00")
    def test_archive_last_updated_30_minutes_ago_bug_6_30(self):
        self._test_test_archive_last_updated_30_minutes_ago()
    
    @time_travel("2025-2-18 6:30:01")
    def test_archive_last_updated_30_minutes_ago_bug_6_30_1(self):
        self._test_test_archive_last_updated_30_minutes_ago()
    
    def _test_test_archive_last_updated_30_minutes_ago(self):
        self.assertEqual(self.default_study.device_settings.resend_period_minutes, 30)
        # should be duplicate test of minimum requirements
        sched_event, archive = self.do_setup_for_resend_with_no_notification_report()
        old_archive_last_updated = archive.last_updated
        old_sched_event_last_updated = sched_event.last_updated
        ArchivedEvent.objects.filter(pk=archive.pk).update(last_updated=self.NOW_SORTA - timedelta(minutes=30))
        self.run_resend_logic_and_refresh_these_models(sched_event, archive)
        self.assert_resend_logic_reenabled_schedule_correctly(sched_event, archive)
        
        # probably overkill or redundant
        self.assertGreater(archive.last_updated, old_archive_last_updated)
        self.assertGreater(sched_event.last_updated, old_sched_event_last_updated)
