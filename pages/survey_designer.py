from flask import abort, Blueprint, render_template

from config.settings import DOMAIN_NAME
from database.study_models import Survey
from libs.admin_authentication import (authenticate_researcher_study_access,
    get_researcher_allowed_studies, researcher_is_an_admin)

survey_designer = Blueprint('survey_designer', __name__)

# TODO: Low Priority. implement "study does not exist" page.
# TODO: Low Priority. implement "survey does not exist" page.


@survey_designer.route('/edit_survey/<string:survey_id>')
@authenticate_researcher_study_access
def render_edit_survey(survey_id=None):
    try:
        survey = Survey.objects.get(pk=survey_id)
    except Survey.DoesNotExist:
        return abort(404)

    s = survey.as_native_python()
    study = survey.study
    return render_template(
        'edit_survey.html',
        survey=survey.as_native_python(),
        study=study,
        allowed_studies=get_researcher_allowed_studies(),
        is_admin=researcher_is_an_admin(),
        domain_name=DOMAIN_NAME,  # used in a Javascript alert, see survey-editor.js
    )
