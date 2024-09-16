from constants.user_constants import ResearcherRole
from database.survey_models import Survey
from tests.common import ResearcherSessionTest


#
## survey api
#


class TestCreateSurvey(ResearcherSessionTest):
    ENDPOINT_NAME = "survey_endpoints.create_survey"
    REDIRECT_ENDPOINT_NAME = "survey_endpoints.render_edit_survey"
    
    def test_tracking(self):
        self._test(Survey.TRACKING_SURVEY)
    
    def test_audio(self):
        self._test(Survey.AUDIO_SURVEY)
    
    def _test(self, survey_type: str):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.assertEqual(Survey.objects.count(), 0)
        resp = self.smart_get_redirect(self.session_study.id, survey_type)
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(Survey.objects.count(), 1)
        survey: Survey = Survey.objects.get()
        self.assertEqual(survey_type, survey.survey_type)


# FIXME: add schedule removal tests to this test
class TestDeleteSurvey(ResearcherSessionTest):
    ENDPOINT_NAME = "survey_endpoints.delete_survey"
    REDIRECT_ENDPOINT_NAME = "study_endpoints.view_study_page"
    
    def test(self):
        self.assertEqual(Survey.objects.count(), 0)
        self.set_session_study_relation(ResearcherRole.researcher)
        survey = self.generate_survey(self.session_study, Survey.TRACKING_SURVEY)
        self.assertEqual(Survey.objects.count(), 1)
        self.smart_post_redirect(self.session_study.id, survey.id)
        self.assertEqual(Survey.objects.count(), 1)
        self.assertEqual(Survey.objects.filter(deleted=False).count(), 0)


# FIXME: implement more details of survey object updates
class TestUpdateSurvey(ResearcherSessionTest):
    ENDPOINT_NAME = "survey_endpoints.update_survey"
    
    def test_with_hax_to_bypass_the_hard_bit(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        survey = self.generate_survey(self.session_study, Survey.TRACKING_SURVEY)
        self.assertEqual(survey.settings, '{}')
        resp = self.smart_post(
            self.session_study.id,
            survey.id,
            content='[]',
            settings='[]',
            weekly_timings='[]',
            absolute_timings='[]',
            relative_timings='[]',
        )
        survey.refresh_from_db()
        self.assertEqual(survey.settings, '[]')
        self.assertEqual(resp.status_code, 201)


# FIXME: add interventions and survey schedules
class TestRenderEditSurvey(ResearcherSessionTest):
    ENDPOINT_NAME = "survey_endpoints.render_edit_survey"
    
    def test(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        survey = self.generate_survey(self.session_study, Survey.TRACKING_SURVEY)
        self.smart_get_status_code(200, self.session_study.id, survey.id)
