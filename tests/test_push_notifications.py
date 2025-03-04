# trunk-ignore-all(ruff/B018)
from django.core.exceptions import ValidationError
from django.utils import timezone

from constants.message_strings import (ACCOUNT_NOT_FOUND, CONNECTION_ABORTED,
    FAILED_TO_ESTABLISH_CONNECTION, UNEXPECTED_SERVICE_RESPONSE, UNKNOWN_REMOTE_ERROR)
from database.schedule_models import AbsoluteSchedule, ArchivedEvent
from services.survey_push_notifications import (create_archived_events, failed_send_survey_handler,
    success_send_survey_handler)
from tests.common import CommonTestCase


class TestPushComponents(CommonTestCase):
    
    # we don't need to test the different ScheduledEvent types for archive, they're all the same.
    def test_create_archived_event_one_absolute_schedule_full_features(self):
        self.set_default_participant_all_push_notification_features
        event = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())
        create_archived_events([event], self.default_participant, "success")
        event.refresh_from_db()
        
        self.assertEqual(ArchivedEvent.objects.count(), 1)
        archive = ArchivedEvent.objects.get()
        
        self.assertTrue(event.deleted)
        self.assertEqual(archive.uuid, event.uuid)
    
    def test_create_archived_event_one_absolute_schedule_no_resends_means_no_uuid_on_archive(self):
        # self.set_working_push_notification_basices  # lol this process does not
        event = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())
        create_archived_events([event], self.default_participant, "success")
        event.refresh_from_db()
        
        self.assertEqual(ArchivedEvent.objects.count(), 1)
        archive = ArchivedEvent.objects.get()
        
        self.assertTrue(event.deleted)
        self.assertIsNone(archive.uuid)
    
    def test_create_archived_event_one_absolute_schedule_failure_doesnt_delete(self):
        self.set_default_participant_all_push_notification_features
        event = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())
        create_archived_events([event], self.default_participant, "literally any string that is not 'success'")
        event.refresh_from_db()
        
        self.assertEqual(ArchivedEvent.objects.count(), 1)
        archive = ArchivedEvent.objects.get()
        
        self.assertFalse(event.deleted) 
        self.assertEqual(archive.uuid, event.uuid)  # should still have a uuid
    
    # if these error behaviors change I want to know.
    def test_create_archived_event_one_absolute_schedule_requirement(self):
        event = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())
        event.absolute_schedule = None
        event.save()
        with self.assertRaisesMessage(Exception, "ScheduledEvent had no associated schedule"):
            create_archived_events([event], self.default_participant, "success")
    
    def test_create_archived_event_one_absolute_schedule_requirement_2(self):
        event = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())
        AbsoluteSchedule.objects.all().delete()
        with self.assertRaises(ValidationError):
            create_archived_events([event], self.default_participant, "success")


# class TestPushNotificationFull(CommonTestCase):
    
#     def test_push_notification(self):
#         # this test is a bit of a mess, it's not really organized and it's not really testing
#         # anything.  It's just a big test that runs a bunch of stuff to make sure it doesn't crash.
#         self.populate_default_fcm_token

# these errors are taken directly from live servers with mild details purged

PUSH_NOTIFICATION_OBSCURE_HTML_ERROR_CONTENT = """
Unexpected HTTP response with status: 502; body: <!DOCTYPE html>
<html lang=en>
  <meta charset=utf-8>
  <meta name=viewport content="initial-scale=1, minimum-scale=1, width=device-width">
  <title>Error 502 (Server Error)!!1</title>
  <style>
    *{margin:0;
    padding:0}html,code{font:15px/22px arial,sans-serif}html{background:#fff;
    color:#222;
    padding:15px}body{margin:7% auto 0;
    max-width:390px;
    min-height:180px;
    padding:30px 0 15px}* > body{background:url(//www.google.com/images/errors/robot.png) 100% 5px no-repeat;
    padding-right:205px}p{margin:11px 0 22px;
    overflow:hidden}ins{color:#777;
    text-decoration:none}a img{border:0}@media screen and (max-width:772px){body{background:none;
    margin-top:0;
    max-width:none;
    padding-right:0}}#logo{background:url(//www.google.com/images/branding/googlelogo/1x/googlelogo_color_150x54dp.png) no-repeat;
    margin-left:-5px}@media only screen and (min-resolution:192dpi){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat 0% 0%/100% 100%;
    -moz-border-image:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) 0}}@media only screen and (-webkit-min-device-pixel-ratio:2){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat;
    -webkit-background-size:100% 100%}}#logo{display:inline-block;
    height:54px;
    width:150px}
  </style>
  <a href=//www.google.com/><span id=logo aria-label=Google></span></a>
  <p><b>502.</b> <ins>That’s an error.</ins>
  <p>The server encountered a temporary error and could not complete your request.<p>Please try again in 30 seconds.  <ins>That’s all we know.</ins>
""".strip()

PUSH_NOTIFICATION_ERROR_INVALID_LENGTH = 'Unknown error while making a remote service call: ("Connection broken: InvalidChunkLength(got length b\'\', 0 bytes read)", InvalidChunkLength(got length b\'\', 0 bytes read))'
PUSH_NOTIFICATION_ERROR_CONNECTION_POOL = "Failed to establish a connection: HTTPSConnectionPool(host='fcm.googleapis.com', port=443): Max retries exceeded with url: /v1/projects/beiwe-20592/messages:send (Caused by ProtocolError('Connection aborted.', RemoteDisconnected('Remote end closed connection without response')))"
PUSH_NOTIFICATION_ERROR_ABORTED = "('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))"
PUSH_NOTIFICATION_INVALID_GRANT = "('invalid_grant: Invalid grant: account not found', {'error': 'invalid_grant', 'error_description': 'Invalid grant: account not found'})"


class TestSendHandlers(CommonTestCase):
    
    def test_weird_html_502_error(self):
        failed_send_survey_handler(
            participant=self.default_participant,
            fcm_token="a",
            error_message=PUSH_NOTIFICATION_OBSCURE_HTML_ERROR_CONTENT,
            schedules=[self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())],
            debug=False,
        )
        archive = ArchivedEvent.objects.get()
        self.assertEqual(archive.status, UNEXPECTED_SERVICE_RESPONSE)
    
    def test_error_invalid_length(self):
        failed_send_survey_handler(
            participant=self.default_participant,
            fcm_token="a",
            error_message=PUSH_NOTIFICATION_ERROR_INVALID_LENGTH,
            schedules=[self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())],
            debug=False,
        )
        archive = ArchivedEvent.objects.get()
        self.assertEqual(archive.status, UNKNOWN_REMOTE_ERROR)
    
    def test_error_connection_pool(self):
        failed_send_survey_handler(
            participant=self.default_participant,
            fcm_token="a",
            error_message=PUSH_NOTIFICATION_ERROR_CONNECTION_POOL,
            schedules=[self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())],
            debug=False,
        )
        archive = ArchivedEvent.objects.get()
        self.assertEqual(archive.status, FAILED_TO_ESTABLISH_CONNECTION)
    
    def test_error_aborted(self):
        failed_send_survey_handler(
            participant=self.default_participant,
            fcm_token="a",
            error_message=PUSH_NOTIFICATION_ERROR_ABORTED,
            schedules=[self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())],
            debug=False,
        )
        archive = ArchivedEvent.objects.get()
        self.assertEqual(archive.status, CONNECTION_ABORTED)
    
    def test_invalid_token(self):
        failed_send_survey_handler(
            participant=self.default_participant,
            fcm_token="a",
            error_message=PUSH_NOTIFICATION_INVALID_GRANT,
            schedules=[self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())],
            debug=False,
        )
        archive = ArchivedEvent.objects.get()
        self.assertEqual(archive.status, ACCOUNT_NOT_FOUND)
    
    #
    ## success handler
    # 
    
    def test_success_handler_no_uuids(self):
        # participant does not meet resend, so no uuids on archives
        event = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())
        success_send_survey_handler(
            participant=self.default_participant,
            fcm_token=self.default_fcm_token.token,
            events=[event],
        )
        
        self.assertEqual(ArchivedEvent.objects.count(), 1)
        archive = ArchivedEvent.objects.get()
        self.assertEqual(archive.uuid, None)
        
        self.assertEqual(archive.status, "success")
        self.assertEqual(archive.participant.pk, self.default_participant.pk)
        self.assertEqual(archive.schedule_type, "absolute")
        self.assertEqual(archive.scheduled_time, event.scheduled_time)
        
        event.refresh_from_db()
        self.assertEqual(event.deleted, True)
    
    def test_success_handler_with_uuids(self):
        self.set_default_participant_all_push_notification_features
        event = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())
        success_send_survey_handler(
            participant=self.default_participant,
            fcm_token=self.default_fcm_token.token,
            events=[event],
        )
        
        self.assertEqual(ArchivedEvent.objects.count(), 1)
        archive = ArchivedEvent.objects.get()
        self.assertEqual(archive.status, "success")
        self.assertEqual(archive.uuid, event.uuid)
        
        self.assertEqual(archive.status, "success")
        self.assertEqual(archive.participant.pk, self.default_participant.pk)
        self.assertEqual(archive.schedule_type, "absolute")
        self.assertEqual(archive.scheduled_time, event.scheduled_time)
        
        event.refresh_from_db()
        self.assertEqual(event.deleted, True)