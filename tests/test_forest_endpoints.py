import uuid
from datetime import date, datetime
from typing import Tuple
from unittest.mock import MagicMock, patch

import dateutil
from django.http import FileResponse

from constants.celery_constants import ForestTaskStatus
from constants.data_stream_constants import GPS
from constants.forest_constants import FOREST_NO_TASK, FOREST_TASK_CANCELLED, ForestTree
from constants.testing_constants import EMPTY_ZIP, SIMPLE_FILE_CONTENTS
from constants.user_constants import ResearcherRole
from database.forest_models import ForestTask, SummaryStatisticDaily
from tests.common import ResearcherSessionTest
from tests.helpers import CURRENT_TEST_DATE_BYTES, DummyThreadPool


#
## forest endpoints
#


# FIXME: This endpoint is unusable, it is not a viable way to look at forest stuff.
class TestForestAnalysisProgress(ResearcherSessionTest):
    ENDPOINT_NAME = "forest_endpoints.forest_tasks_progress"
    
    def test(self):
        # hey it loads...
        self.set_session_study_relation(ResearcherRole.researcher)
        for _ in range(10):
            self.generate_participant(self.session_study)
        # print(Participant.objects.count())
        self.smart_get(self.session_study.id)


# class TestForestCreateTasks(ResearcherSessionTest):
#     ENDPOINT_NAME = "forest_endpoints.create_tasks"
#     def test(self):
#         self.smart_get()


# class TestForestTaskLog(ResearcherSessionTest):
#     ENDPOINT_NAME = "forest_endpoints.task_log"
#     def test(self):
#         self.smart_get()


# TODO: make a test for whether a tsudy admin can hit this endpoint? I think there's a bug in the authenticate_admin decorator that allows that.....
class TestForestDownloadTaskLog(ResearcherSessionTest):
    ENDPOINT_NAME = "forest_endpoints.download_task_log"
    REDIRECT_ENDPOINT_NAME = ResearcherSessionTest.IGNORE_THIS_ENDPOINT
    
    header_row = "Created On,Data Date End,Data Date Start,Id,Forest Tree,"\
        "Forest Output Exists,Patient Id,Process Start Time,Process Download End Time,"\
        "Process End Time,Status,Total File Size\r\n"
    
    def test_no_relation_no_site_admin_no_worky(self):
        # this streams a csv of tasks on a study
        resp = self.smart_get_status_code(403, self.session_study.id)
        self.assertEqual(resp.content, b"")
    
    def test_researcher_no_worky(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_get_status_code(403, self.session_study.id)
        self.assertEqual(resp.content, b"")
    
    def test_study_admin_no_worky(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        resp = self.smart_get_status_code(403, self.session_study.id)
        self.assertEqual(resp.content, b"")
    
    def test_site_admin_can(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        resp = self.smart_get_status_code(200, self.session_study.id)
        self.assertEqual(b"".join(resp.streaming_content).decode(), self.header_row)
    
    def test_single_forest_task(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.default_forest_task.update(
            created_on=datetime(2020, 1, 1, tzinfo=dateutil.tz.UTC),
            data_date_start=date(2020, 1, 1),
            data_date_end=date(2020, 1, 5),
            forest_tree=ForestTree.jasmine,
            forest_output_exists=True,
            process_start_time=datetime(2020, 1, 1, tzinfo=dateutil.tz.UTC),  # midnight
            process_download_end_time=datetime(2020, 1, 1, 1, tzinfo=dateutil.tz.UTC),  # 1am
            process_end_time=datetime(2020, 1, 1, 2, tzinfo=dateutil.tz.UTC),  # 2am
            status=ForestTaskStatus.success,
            total_file_size=123456789,
        )
        
        resp = self.smart_get_status_code(200, self.session_study.id)
        content = b"".join(resp.streaming_content).decode()
        self.assertEqual(content.count("\r\n"), 2)
        line = content.splitlines()[1]
        
        # 12 columns, 11 commas
        self.assertEqual(line.count(","), 11)
        items = line.split(",")
        self.assertEqual(items[0], "2020-01-01 00:00 (UTC)")                   # Created On
        self.assertEqual(items[1], "2020-01-05")                               # Data Date End
        self.assertEqual(items[2], "2020-01-01")                               # Data Date Start
        self.assertEqual(items[3], str(self.default_forest_task.external_id))  # duh
        self.assertEqual(items[4], "Jasmine")                                  # Forest Tree
        self.assertEqual(items[5], "Yes")                                      # Forest Output Exists
        self.assertEqual(items[6], "patient1")                                 # Patient Id
        self.assertEqual(items[7], "2020-01-01 00:00 (UTC)")                   # Process Start Time
        self.assertEqual(items[8], "2020-01-01 01:00 (UTC)")                   # Process Download End Time
        self.assertEqual(items[9], "2020-01-01 02:00 (UTC)")                   # Process End Time
        self.assertEqual(items[10], "success")                                 # Status
        self.assertEqual(items[11], "123456789")                               # Total File Size


class TestForestCancelTask(ResearcherSessionTest):
    ENDPOINT_NAME = "forest_endpoints.cancel_task"
    REDIRECT_ENDPOINT_NAME = "forest_endpoints.task_log"
    
    def test_no_relation_cannot(self):
        self.smart_post_status_code(403, self.session_study.id, self.default_forest_task.external_id)
        self.default_forest_task.refresh_from_db()
        self.assertEqual(self.default_forest_task.status, ForestTaskStatus.queued)
    
    def test_researcher_cannot(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_post_status_code(403, self.session_study.id, self.default_forest_task.external_id)
        self.default_forest_task.refresh_from_db()
        self.assertEqual(self.default_forest_task.status, ForestTaskStatus.queued)
    
    def test_study_admin_cannot(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_post_status_code(403, self.session_study.id, self.default_forest_task.external_id)
        self.default_forest_task.refresh_from_db()
        self.assertEqual(self.default_forest_task.status, ForestTaskStatus.queued)
    
    def test_site_admin_can(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_post_redirect(self.session_study.id, self.default_forest_task.external_id)
        self.default_forest_task.refresh_from_db()
        self.assertEqual(self.default_forest_task.status, ForestTaskStatus.cancelled)
        self.assert_present(
            FOREST_TASK_CANCELLED, self.redirect_get_contents(self.session_study.id)
        )
    
    def test_bad_uuid(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_post_redirect(self.session_study.id, "abc123")
        self.assert_present(FOREST_NO_TASK, self.redirect_get_contents(self.session_study.id))
        # and then a working wrong one
        self.smart_post_redirect(self.session_study.id, uuid.uuid4())
        self.assert_present(FOREST_NO_TASK, self.redirect_get_contents(self.session_study.id))


class TestForestDownloadOutput(ResearcherSessionTest):
    ENDPOINT_NAME = "forest_endpoints.download_output_data"
    REDIRECT_ENDPOINT_NAME = ResearcherSessionTest.IGNORE_THIS_ENDPOINT
    
    def test_no_relation_cannot(self):
        self.smart_get_status_code(403, self.session_study.id, self.default_forest_task.external_id)
    
    def test_researcher_cannot(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_get_status_code(403, self.session_study.id, self.default_forest_task.external_id)
    
    @patch("libs.utils.forest_utils.s3_retrieve")
    def test_study_admin_can(self, s3_retrieve: MagicMock):
        s3_retrieve.return_value = SIMPLE_FILE_CONTENTS
        self.set_session_study_relation(ResearcherRole.study_admin)
        resp = self.smart_get_status_code(
            200, self.session_study.id, self.default_forest_task.external_id)
        self.assertEqual(resp.content, SIMPLE_FILE_CONTENTS)
    
    @patch("libs.utils.forest_utils.s3_retrieve")
    def test_site_admin_can(self, s3_retrieve: MagicMock):
        s3_retrieve.return_value = SIMPLE_FILE_CONTENTS
        self.set_session_study_relation(ResearcherRole.site_admin)
        resp = self.smart_get_status_code(
            200, self.session_study.id, self.default_forest_task.external_id)
        self.assertEqual(resp.content, SIMPLE_FILE_CONTENTS)
    
    def test_404(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_get_status_code(404, self.session_study.id, "abc123")
        self.smart_get_status_code(404, self.session_study.id, uuid.uuid4())


class TestForestDownloadTaskData(ResearcherSessionTest):
    # you need to look at tests/test_data_access_api.py to understand the weird problems that can
    # happen with this use of the data access api code. I'm not redocumenting it.
    
    ENDPOINT_NAME = "forest_endpoints.download_task_data"
    REDIRECT_ENDPOINT_NAME = ResearcherSessionTest.IGNORE_THIS_ENDPOINT
    
    def test_no_relation_cannot(self):
        self.smart_get_status_code(403, self.session_study.id, self.default_forest_task.external_id)
    
    def test_researcher_cannot(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_get_status_code(403, self.session_study.id, self.default_forest_task.external_id)
    
    def test_study_admin_can(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.smart_get_status_code(200, self.session_study.id, self.default_forest_task.external_id)
    
    def test_site_admin_can(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        resp: FileResponse = self.smart_get_status_code(
            200, self.session_study.id, self.default_forest_task.external_id)
        self.assertEqual(b"".join(resp.streaming_content), EMPTY_ZIP)
    
    def no_such_task(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_get_status_code(404, self.session_study.id, uuid.uuid4())
    
    @patch("libs.streaming_zip.s3_retrieve")
    @patch("libs.streaming_zip.ThreadPool")  # see tests/test_data_access_api.py
    def test_full_request(self, threadpool: MagicMock, s3_retrieve: MagicMock):
        threadpool.return_value = DummyThreadPool()
        s3_retrieve.return_value = SIMPLE_FILE_CONTENTS
        
        self.set_session_study_relation(ResearcherRole.site_admin)
        # make a jasmine task and a single file within the time range that should download (gps)
        self.default_forest_task.update(
            data_date_start=datetime(2020, 1, 1, tzinfo=dateutil.tz.UTC),
            data_date_end=datetime(2020, 1, 4, tzinfo=dateutil.tz.UTC),
            forest_tree=ForestTree.jasmine,
        )
        self.default_chunkregistry.update(
            time_bin=datetime(2020, 1, 2, tzinfo=dateutil.tz.UTC), data_type=GPS
        )
        # hit endpoint, check for our SIMPLE_FILE_CONTENTS nonce
        resp: FileResponse = self.smart_get_status_code(
            200, self.session_study.id, self.default_forest_task.external_id
        )
        self.assertIn(SIMPLE_FILE_CONTENTS, b"".join(resp.streaming_content))


class TestRerunForestTask(ResearcherSessionTest):
    ENDPOINT_NAME = "forest_endpoints.copy_forest_task"
    REDIRECT_ENDPOINT_NAME = "forest_endpoints.task_log"
    
    def test_no_task_specified(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_post_redirect(self.session_study.id)
        self.assert_present(FOREST_NO_TASK, self.redirect_get_contents(self.session_study.id))
    
    def test_no_such_task(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_post_redirect(self.session_study.id, external_id=uuid.uuid4())
        self.assert_present(FOREST_NO_TASK, self.redirect_get_contents(self.session_study.id))
    
    def test_bad_uuid(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_post_redirect(self.session_study.id, external_id="abc123")
        self.assert_present(FOREST_NO_TASK, self.redirect_get_contents(self.session_study.id))
    
    def test_success(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        old_task = self.default_forest_task
        self.assertEqual(ForestTask.objects.count(), 1)
        self.smart_post_redirect(self.session_study.id, external_id=old_task.external_id)
        self.assertEqual(ForestTask.objects.count(), 2)
        new_task = ForestTask.objects.exclude(id=old_task.id).get()
        msg = f"Made a copy of {old_task.external_id} with id {new_task.external_id}."
        self.assert_present(msg, self.redirect_get_contents(self.session_study.id))
    
    def test_researcher_cannot(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        old_task = self.default_forest_task
        self.assertEqual(ForestTask.objects.count(), 1)
        self.smart_post_status_code(403, self.session_study.id, external_id=old_task.external_id)
        self.assertEqual(ForestTask.objects.count(), 1)
        self.smart_post_status_code(403, self.session_study.id)
    
    def test_study_admin_cannot(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        old_task = self.default_forest_task
        self.assertEqual(ForestTask.objects.count(), 1)
        self.smart_post_status_code(403, self.session_study.id, external_id=old_task.external_id)
        self.assertEqual(ForestTask.objects.count(), 1)
        self.smart_post_status_code(403, self.session_study.id)


class TestDownloadSummaryStatisticsSummary(ResearcherSessionTest):
    ENDPOINT_NAME = "forest_endpoints.download_summary_statistics_csv"
    
    # edit this to match the csv header if it is updated
    CSV_HEADER: bytes = ",".join((
        'Date',
        'Participant Id',
        'Study Id',
        'Timezone',
        'Accelerometer Bytes',
        'Ambient Audio Bytes',
        'App Log Bytes',
        'Bluetooth Bytes',
        'Calls Bytes',
        'Devicemotion Bytes',
        'Gps Bytes',
        'Gyro Bytes',
        'Identifiers Bytes',
        'Ios Log Bytes',
        'Magnetometer Bytes',
        'Power State Bytes',
        'Proximity Bytes',
        'Reachability Bytes',
        'Survey Answers Bytes',
        'Survey Timings Bytes',
        'Texts Bytes',
        'Audio Recordings Bytes',
        'Wifi Bytes',
        'Distance Diameter',
        'Distance From Home',
        'Distance Traveled',
        'Flight Distance Average',
        'Flight Distance Stddev',
        'Flight Duration Average',
        'Flight Duration Stddev',
        'Gps Data Missing Duration',
        'Home Duration',
        'Gyration Radius',
        'Significant Location Count',
        'Significant Location Entropy',
        'Pause Time',
        'Obs Duration',
        'Obs Day',
        'Obs Night',
        'Total Flight Time',
        'Av Pause Duration',
        'Sd Pause Duration',
        'Incoming Text Count',
        'Incoming Text Degree',
        'Incoming Text Length',
        'Outgoing Text Count',
        'Outgoing Text Degree',
        'Outgoing Text Length',
        'Incoming Text Reciprocity',
        'Outgoing Text Reciprocity',
        'Outgoing Mms Count',
        'Incoming Mms Count',
        'Incoming Call Count',
        'Incoming Call Degree',
        'Incoming Call Duration',
        'Outgoing Call Count',
        'Outgoing Call Degree',
        'Outgoing Call Duration',
        'Missed Call Count',
        'Missed Callers',
        'Uniq Individual Call Or Text Count',
        'Total Surveys',
        'Total Completed Surveys',
        'Total Opened Surveys',
        'Average Time To Submit',
        'Average Time To Open',
        'Average Duration',
        'Walking Time',
        'Steps',
        'Cadence',
    )).encode() + b"\r\n"
    
    @property
    def EMPTY_PARTICIPANT(self) -> bytes:
        """ The csv contains data from our default serializable fields which have this increasing
        numeric pattern, they vary between ints and floats inconsistently. """
        return (
            f"{self.CURRENT_DATE.isoformat()},"
            f"{self.default_participant.patient_id},"
            f"{self.default_study.object_id},"
            "5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25.0,26.0,27.0,28.0,29.0,30.0"
            ",31.0,32,33.0,34.0,35,36.0,37,38.0,39.0,40.0,41.0,42.0,43.0,44,45,46,47,48,49,50,51,52,"
            "53,54,55,56.0,57,58,59.0,60,61,62,63,64,65,66.0,67.0,68.0,69.0,70.0,71.0\r\n"
            .encode()
        )
    
    def test_no_relation_no_worky(self):
        self.smart_get_status_code(403, self.session_study.id)
    
    def test_researcher_no_worky(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_get_status_code(403, self.session_study.id)
    
    def test_study_admin_can(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.smart_get_status_code(200, self.session_study.id)
    
    def test_site_admin_can(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_get_status_code(200, self.session_study.id)
    
    def test_no_participants(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        resp = self.smart_get_status_code(200, self.session_study.id)
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, self.CSV_HEADER)# + self.EMPTY_GRAND_TOTALS + self.EMPTY_GLOBALS)
    
    def test_one_participant_no_data(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.using_default_participant()
        resp = self.smart_get_status_code(200, self.session_study.id)
        correct = self.CSV_HEADER
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, correct)
    
    def test_one_participant_single_summarystatistic(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.using_default_participant()
        self.default_summary_statistic_daily
        resp = self.smart_get_status_code(200, self.session_study.id)
        correct = self.CSV_HEADER + self.EMPTY_PARTICIPANT
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, correct)
    
    def test_two_summary_statistics(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.using_default_participant()
        self.default_summary_statistic_daily
        p2 = self.generate_participant(self.session_study, patient_id="patient2")
        self.generate_summary_statistic_daily(date(2020,1,1))
        resp = self.smart_get_status_code(200, self.session_study.id)
        correct = b"".join((
            self.CSV_HEADER,
            self.EMPTY_PARTICIPANT.replace(CURRENT_TEST_DATE_BYTES, b"2020-01-01"),
            self.EMPTY_PARTICIPANT,
        ))
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, correct)
    
    def test_two_participants(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.using_default_participant()
        p2 = self.generate_participant(self.session_study, patient_id="patient2")
        self.default_summary_statistic_daily
        self.generate_summary_statistic_daily(date(2020,1,1), p2)
        resp = self.smart_get_status_code(200, self.session_study.id)        
        correct = b"".join((
            self.CSV_HEADER,
            self.EMPTY_PARTICIPANT,
            self.EMPTY_PARTICIPANT.replace(b"patient1", b"patient2")
                .replace(CURRENT_TEST_DATE_BYTES, b"2020-01-01")
        ))
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, correct)


class TestDownloadParticipantTreeData(ResearcherSessionTest):
    ENDPOINT_NAME = "forest_endpoints.download_participant_tree_data"
    
    summary_field_tree_map = {
        ForestTree.jasmine: "jasmine_task",
        ForestTree.oak: "oak_task",
        ForestTree.sycamore: "sycamore_task",
        ForestTree.willow: "willow_task",
    }
    
    # hardcode these, but generate with
    # b"Date," + ",".join(JASMINE_FIELDS).replace("jasmine_", "").replace("_", " ").title().encode()
    # b"Date," + ",".join(OAK_FIELDS).replace("oak_", "").replace("_", " ").title().encode()
    # b"Date," + ",".join(SYCAMORE_FIELDS).replace("sycamore_", "").replace("_", " ").title().encode()
    # b"Date," + ",".join(WILLOW_FIELDS).replace("willow_", "").replace("_", " ").title().encode()
    csv_columns_map = {
        ForestTree.jasmine:
            b'Date,Distance Diameter,Distance From Home,Distance Traveled,Flight Distance Average,'
            b'Flight Distance Stddev,Flight Duration Average,Flight Duration Stddev,Gps Data '
            b'Missing Duration,Home Duration,Gyration Radius,Significant Location Count,Significant'
            b' Location Entropy,Pause Time,Obs Duration,Obs Day,Obs Night,Total Flight Time,Av'
            b' Pause Duration,Sd Pause Duration',
        ForestTree.oak:
            b'Date,Walking Time,Steps,Cadence',
        ForestTree.sycamore:
            b'Date,Total Surveys,Total Completed Surveys,Total Opened Surveys,Average Time To Submit,'
            b'Average Time To Open,Average Duration',
        ForestTree.willow:
            b'Date,Incoming Text Count,Incoming Text Degree,Incoming Text Length,Outgoing Text Count,'
            b'Outgoing Text Degree,Outgoing Text Length,Incoming Text Reciprocity,Outgoing Text'
            b' Reciprocity,Outgoing Mms Count,Incoming Mms Count,Incoming Call Count,Incoming'
            b' Call Degree,Incoming Call Duration,Outgoing Call Count,Outgoing Call Degree,Outgoing'
            b' Call Duration,Missed Call Count,Missed Callers,Uniq Individual Call Or Text Count'
    }
    
    # the values for these are permuted in self.default_summary_statistic_daily_cheatsheet
    # (we do this to create a different value in every field so that we can test and have
    # something visibly wrong.)
    csv_data_line_map = {
        ForestTree.jasmine: CURRENT_TEST_DATE_BYTES + 
            b",25.0,26.0,27.0,28.0,29.0,30.0,31.0,32,33.0,34.0,35,36.0,37,38.0,39.0,40.0,41.0,42.0,43.0",
        ForestTree.oak: CURRENT_TEST_DATE_BYTES + 
            b',69.0,70.0,71.0',
        ForestTree.sycamore: CURRENT_TEST_DATE_BYTES + 
            b',63,64,65,66.0,67.0,68.0',
        ForestTree.willow: CURRENT_TEST_DATE_BYTES + 
            b',44,45,46,47,48,49,50,51,52,53,54,55,56.0,57,58,59.0,60,61,62'
    }
    
    def setup_valid_tree_and_summary_statistic(self, tree_name: str) -> Tuple[ForestTask, SummaryStatisticDaily]:
        task = self.default_forest_task
        task.update(forest_tree=tree_name)
        stat = self.default_summary_statistic_daily
        # we need to point at a correctly typed tree
        stat.update_only(**{self.summary_field_tree_map[tree_name]: task})
        return task, stat
    
    def test_no_relation_no_worky(self):
        task, stat = self.setup_valid_tree_and_summary_statistic(ForestTree.jasmine)
        self.smart_get_status_code(403, self.session_study.id, task.external_id)
    
    def test_researcher_no_worky(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        task, stat = self.setup_valid_tree_and_summary_statistic(ForestTree.jasmine)
        self.smart_get_status_code(403, self.session_study.id, task.external_id)
    
    def test_study_admin_can(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        task, stat = self.setup_valid_tree_and_summary_statistic(ForestTree.jasmine)
        self.smart_get_status_code(200, self.session_study.id, task.external_id)
    
    def test_site_admin_can(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        task, stat = self.setup_valid_tree_and_summary_statistic(ForestTree.jasmine)
        self.smart_get_status_code(200, self.session_study.id, task.external_id)
    
    def test_wrong_study_id(self):
        task, stat = self.setup_valid_tree_and_summary_statistic(ForestTree.jasmine)
        self.set_session_study_relation(ResearcherRole.study_admin)
        # set up a second study so that normal validation passes and we hit our special 404 clause
        study2 = self.generate_study("whatever")
        # set admin relation on the second study
        self.generate_study_relation(self.session_researcher, study2, ResearcherRole.study_admin)
        resp = self.smart_get_status_code(404, study2.id, task.external_id)
        self.assertEqual(resp.content, b"correct 404 case")
    
    def test_each_summary_statistic(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        # jasmine gets overwritten in the for loop
        task, stat = self.setup_valid_tree_and_summary_statistic(ForestTree.jasmine)
        
        # update the tree type and compare output to correct data
        for tree_name in ForestTree.values():
            task.update_only(forest_tree=tree_name)
            resp = self.smart_get_status_code(200, self.session_study.id, task.external_id)
            
            # painful to debug, keep these extra variables
            content = b"".join(resp.streaming_content)
            header, line, split_backslash_r_backslash_n = content.split(b"\r\n")
            self.assertEqual(split_backslash_r_backslash_n, b"")
            correct_line = self.csv_data_line_map[tree_name]
            correct_header = self.csv_columns_map[tree_name]
            self.assertEqual(header, correct_header)
            self.assertEqual(line, correct_line)
    
    def test_no_summary_statistics(self):
        # unlikely to be encountered, but we handle in the request body because otherwise we get an
        # empty string and that could screw someone up.
        self.set_session_study_relation(ResearcherRole.study_admin)
        task, stat = self.setup_valid_tree_and_summary_statistic(ForestTree.jasmine)
        stat.delete()
        resp = self.smart_get_status_code(200, self.session_study.id, task.external_id)
        header, split_backslash_r_backslash_n = resp.content.split(b"\r\n")
        correct_header = self.csv_columns_map[ForestTree.jasmine]
        self.assertEqual(header, correct_header)
        self.assertEqual(split_backslash_r_backslash_n, b"")
    
    def test_two_data_lines(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        task, stat = self.setup_valid_tree_and_summary_statistic(ForestTree.jasmine)
        stat2 = self.generate_summary_statistic_daily(date(2020,1,1))
        
        # we need to point at a correctly typed tree
        stat2.update_only(**{self.summary_field_tree_map[ForestTree.jasmine]: task})
        
        # get the data, esure it has two lines of data
        # painful to debug, keep the extra variables
        resp = self.smart_get_status_code(200, self.session_study.id, task.external_id)
        content = b"".join(resp.streaming_content)
        correct_line = self.csv_data_line_map[ForestTree.jasmine]
        header, line1, line2, split_backslash_r_backslash_n = content.split(b"\r\n")
        self.assertEqual(split_backslash_r_backslash_n, b"")
        self.assertEqual(header, self.csv_columns_map[ForestTree.jasmine])
        self.assertEqual(line2, correct_line)
        self.assertEqual(line1, correct_line.replace(CURRENT_TEST_DATE_BYTES, b"2020-01-01"))
