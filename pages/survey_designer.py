from django.utils import timezone
from django.utils.timezone import localtime
from flask import abort, Blueprint, render_template

from authentication.admin_authentication import (authenticate_researcher_study_access,
    get_researcher_allowed_studies, researcher_is_an_admin)
from config.settings import DOMAIN_NAME
from database.survey_models import Survey
from libs.firebase_config import check_firebase_instance


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

    return render_template(
        'edit_survey.html',
        survey=survey.as_unpacked_native_python(),
        study=survey.study,
        allowed_studies=get_researcher_allowed_studies(),
        is_admin=researcher_is_an_admin(),
        domain_name=DOMAIN_NAME,  # used in a Javascript alert, see survey-editor.js
        interventions_dict={
            intervention.id: intervention.name for intervention in survey.study.interventions.all()
        },
        weekly_timings=survey.weekly_timings(),
        relative_timings=survey.relative_timings(),
        absolute_timings=survey.absolute_timings(),
        push_notifications_enabled=check_firebase_instance(require_android=True) or \
                                   check_firebase_instance(require_ios=True),
        today=localtime(timezone.now(), survey.study.timezone).strftime('%Y-%m-%d'),

    )
