from datetime import date, timedelta
from typing import List

from django.http.response import HttpResponse

from constants.data_stream_constants import COMPLETE_DATA_STREAM_DICT, DASHBOARD_DATA_STREAMS
from constants.forest_constants import DATA_QUANTITY_FIELD_MAP
from constants.user_constants import ResearcherRole
from database.forest_models import SummaryStatisticDaily
from database.security_models import ApiKey
from database.user_models_participant import Participant
from tests.common import ResearcherSessionTest


#
## data_access_web_form
#
class TestDataAccessWebFormPage(ResearcherSessionTest):
    ENDPOINT_NAME = "data_page_endpoints.data_api_web_form_page"
    
    def test(self):
        resp = self.smart_get()
        self.assert_present("can download data. Go to", resp.content)
        
        api_key = ApiKey.generate(researcher=self.session_researcher)
        id_key, secret_key = api_key.access_key_id, api_key.access_key_secret_plaintext
        
        resp = self.smart_get()
        self.assert_not_present("can download data. Go to", resp.content)


#
## dashboard pages
#


class TestDashboard(ResearcherSessionTest):
    ENDPOINT_NAME = "data_page_endpoints.dashboard_page"
    
    def assert_data_streams_present(self, resp: HttpResponse):
        for data_stream_text in COMPLETE_DATA_STREAM_DICT.values():
            self.assert_present(data_stream_text, resp.content)
    
    def test_dashboard_no_participants(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_get_status_code(200, str(self.session_study.id))
        self.assert_present("Choose a participant or data stream to view", resp.content)
        self.assert_not_present(self.DEFAULT_PARTICIPANT_NAME, resp.content)
        self.assert_data_streams_present(resp)
    
    def test_dashboard_one_participant(self):
        self.using_default_participant()
        # default user and default study already instantiated
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_get_status_code(200, str(self.session_study.id))
        self.assert_present("Choose a participant or data stream to view", resp.content)
        self.assert_present(self.DEFAULT_PARTICIPANT_NAME, resp.content)
        self.assert_data_streams_present(resp)
    
    def test_dashboard_many_participant(self):
        participants = self.generate_10_default_participants
        # default user and default study already instantiated
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_get_status_code(200, str(self.session_study.id))
        self.assert_present("Choose a participant or data stream to view", resp.content)
        for p in participants:
            self.assert_present(p.patient_id, resp.content)
        self.assert_data_streams_present(resp)


class TestDashboardStream(ResearcherSessionTest):
    ENDPOINT_NAME = "data_page_endpoints.get_data_for_dashboard_datastream_display"
    
    def test_no_participant(self):
        self.do_data_stream_test(create_summaries=False, number_participants=0)
    
    def test_one_participant_no_data(self):
        self.do_data_stream_test(create_summaries=False, number_participants=1)
    
    def test_three_participants_no_data(self):
        self.do_data_stream_test(create_summaries=False, number_participants=3)
    
    def test_five_participants_with_data(self):
        self.do_data_stream_test(create_summaries=True, number_participants=5)
    
    def do_data_stream_test(self, create_summaries=False, number_participants=1):
        # this is slow because it make SO MANY REQUESTS
        
        # self.default_participant  < -- breaks, collision with default name.
        self.set_session_study_relation()
        
        # create all the participants we need, populate some summaries
        participants: List[Participant] = [
            self.generate_participant(self.session_study, patient_id=f"patient{i+1}")
            for i in range(number_participants)
        ]
        
        if create_summaries:
            for participant in participants:
                self.generate_summary_statistic_daily(
                    a_date=date.today(),
                    participant=participant,
                )
        
        # technically the endpoint accepts post and get. We don't care enouhg to test both.
        byte_count_match_by_field_name = self.default_summary_statistic_daily_cheatsheet()
        
        for data_stream in DASHBOARD_DATA_STREAMS:
            html = self.smart_get_status_code(200, self.session_study.id, data_stream).content
            
            # get the byte count for the data stream, populate some html
            byte_count = byte_count_match_by_field_name[DATA_QUANTITY_FIELD_MAP[data_stream]]
            x = f'calculateColor({byte_count})" data-number="{byte_count}">{byte_count}</td>'.encode()
            
            title = COMPLETE_DATA_STREAM_DICT[data_stream]  # explodes if everything is broken
            self.assert_present(title, html)
            
            for i, participant in enumerate(participants, start=0):
                if create_summaries:
                    self.assert_present(participant.patient_id, html)
                    self.assert_not_present("There is no data currently available for", html)
                    self.assertEqual(html.count(x), number_participants)
                else:
                    self.assert_not_present(participant.patient_id, html)
                    self.assert_present(f"There is no data currently available for {title}", html)
            
            if not participants or not create_summaries:
                self.assert_present(f"There is no data currently available for {title}", html)


# FIXME: this page renders with almost no data
class TestDashboardParticipantDisplay(ResearcherSessionTest):
    ENDPOINT_NAME = "data_page_endpoints.dashboard_participant_page"
    
    def test_participant_display_no_data(self):
        self.set_session_study_relation()
        resp = self.smart_get_status_code(
            200, self.session_study.id, self.default_participant.patient_id
        )
        self.assert_present(
            "There is no data currently available for patient1 of Study", resp.content
        )
    
    def test_five_participants_with_data(self):
        self.set_session_study_relation()
        
        for i in range(10):
            self.generate_summary_statistic_daily(
                a_date=date.today() - timedelta(days=i),
                participant=self.default_participant,
            )
        
        # need to be post and get requests, it was just built that way
        html1 = self.smart_get_status_code(
            200, self.session_study.id, self.default_participant.patient_id
        ).content
        html2 = self.smart_post_status_code(
            200, self.session_study.id, self.default_participant.patient_id
        ).content
        
        # sanity check that the number of fields has not changed (update test if they do)
        field_names = [f.name for f in SummaryStatisticDaily._meta.local_fields 
                       if f.name.startswith("beiwe_") and f.name.endswith("_bytes")] 
        self.assertEqual(len(field_names), 19)
        
        # there should be 7 of each byte count one for each day in the forced 7 day period, from 8
        # to 25 based on the summary statistic cheat sheet.
        for i in range(6, 25):
            self.assertEqual(html1.count(f'<td class="bytes"> {i} </td>'.encode()), 7)
            self.assertEqual(html2.count(f'<td class="bytes"> {i} </td>'.encode()), 7)
        
        for title in COMPLETE_DATA_STREAM_DICT.values():
            self.assert_present(title, html1)
            self.assert_present(title, html2)
        
        self.assert_present(self.default_participant.patient_id, html1)
        self.assert_present(self.default_participant.patient_id, html2)
