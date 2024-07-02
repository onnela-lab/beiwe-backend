from django.shortcuts import redirect, render
from django.views.decorators.http import require_GET

from authentication.admin_authentication import (authenticate_researcher_login,
    authenticate_researcher_study_access, get_researcher_allowed_studies_as_query_set)
from constants.common_constants import DISPLAY_TIME_FORMAT
from constants.user_constants import ResearcherRole
from database.study_models import Study
from database.user_models_researcher import StudyRelation
from libs.firebase_config import check_firebase_instance
from libs.internal_types import ResearcherRequest
from pages.admin_pages import conditionally_display_study_status_warnings


@require_GET
@authenticate_researcher_login
def choose_study_page(request: ResearcherRequest):
    allowed_studies = get_researcher_allowed_studies_as_query_set(request)
    # If the admin is authorized to view exactly 1 study, redirect to that study,
    # Otherwise, show the "Choose Study" page
    if allowed_studies.count() == 1:
        return redirect('/view_study/{:d}'.format(allowed_studies.values_list('pk', flat=True).get()))

    return render(
        request,
        'choose_study.html',
        context=dict(
            studies=list(allowed_studies.values("name", "id")),
            is_admin=request.session_researcher.is_an_admin(),
        )
    )


@require_GET
@authenticate_researcher_study_access
def view_study_page(request: ResearcherRequest, study_id=None):
    study: Study = Study.objects.get(pk=study_id)

    def get_survey_info(survey_type: str):
        survey_info = list(
            study.surveys.filter(survey_type=survey_type, deleted=False)
            .values('id', 'object_id', 'name', "last_updated")
        )
        for info in survey_info:
            info["last_updated"] = \
                 info["last_updated"].astimezone(study.timezone).strftime(DISPLAY_TIME_FORMAT)
        return survey_info

    is_study_admin = StudyRelation.objects.filter(
        researcher=request.session_researcher, study=study, relationship=ResearcherRole.study_admin
    ).exists()

    conditionally_display_study_status_warnings(request, study)

    return render(
        request,
        template_name='view_study.html',
        context=dict(
            study=study,
            participants_ever_registered_count=study.participants.exclude(os_type='').count(),
            audio_survey_info=get_survey_info('audio_survey'),
            tracking_survey_info=get_survey_info('tracking_survey'),
            # these need to be lists because they will be converted to json.
            study_fields=list(study.fields.all().values_list('field_name', flat=True)),
            interventions=list(study.interventions.all().values_list("name", flat=True)),
            page_location='view_study',
            study_id=study_id,
            is_study_admin=is_study_admin,
            push_notifications_enabled=check_firebase_instance(require_android=True) or
                                       check_firebase_instance(require_ios=True),
        )
    )