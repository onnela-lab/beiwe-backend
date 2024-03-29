import uuid
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import dateutil
from django.http import FileResponse
from django.http.response import FileResponse

from constants.celery_constants import ForestTaskStatus
from constants.data_stream_constants import GPS
from constants.forest_constants import FOREST_NO_TASK, FOREST_TASK_CANCELLED, ForestTree
from constants.testing_constants import EMPTY_ZIP, SIMPLE_FILE_CONTENTS
from constants.user_constants import ResearcherRole
from database.forest_models import ForestTask
from tests.common import ResearcherSessionTest
from tests.helpers import DummyThreadPool


#
## forest_pages
#


# FIXME: make a real test...
class TestForestAnalysisProgress(ResearcherSessionTest):
    ENDPOINT_NAME = "forest_pages.forest_tasks_progress"
    
    def test(self):
        # hey it loads...
        self.set_session_study_relation(ResearcherRole.researcher)
        for _ in range(10):
            self.generate_participant(self.session_study)
        # print(Participant.objects.count())
        self.smart_get(self.session_study.id)



# class TestForestCreateTasks(ResearcherSessionTest):
#     ENDPOINT_NAME = "forest_pages.create_tasks"
#     def test(self):
#         self.smart_get()


# class TestForestTaskLog(ResearcherSessionTest):
#     ENDPOINT_NAME = "forest_pages.task_log"
#     def test(self):
#         self.smart_get()


# TODO: make a test for whether a tsudy admin can hit this endpoint? I think there's a bug in the authenticate_admin decorator that allows that.....
class TestForestDownloadTaskLog(ResearcherSessionTest):
    ENDPOINT_NAME = "forest_pages.download_task_log"
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
    ENDPOINT_NAME = "forest_pages.cancel_task"
    REDIRECT_ENDPOINT_NAME = "forest_pages.task_log"
    
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
    ENDPOINT_NAME = "forest_pages.download_output_data"
    REDIRECT_ENDPOINT_NAME = ResearcherSessionTest.IGNORE_THIS_ENDPOINT
    
    def test_no_relation_cannot(self):
        self.smart_get_status_code(403, self.session_study.id, self.default_forest_task.external_id)
    
    def test_researcher_cannot(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_get_status_code(403, self.session_study.id, self.default_forest_task.external_id)
    
    @patch("libs.forest_utils.s3_retrieve")
    def test_study_admin_can(self, s3_retrieve: MagicMock):
        s3_retrieve.return_value = SIMPLE_FILE_CONTENTS
        self.set_session_study_relation(ResearcherRole.study_admin)
        resp = self.smart_get_status_code(
            200, self.session_study.id, self.default_forest_task.external_id)
        self.assertEqual(resp.content, SIMPLE_FILE_CONTENTS)
    
    @patch("libs.forest_utils.s3_retrieve")
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
    
    ENDPOINT_NAME = "forest_pages.download_task_data"
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
    ENDPOINT_NAME = "forest_pages.copy_forest_task"
    REDIRECT_ENDPOINT_NAME = "forest_pages.task_log"
    
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
