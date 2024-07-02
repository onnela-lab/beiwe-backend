from django.shortcuts import redirect, render
from django.views.decorators.http import require_GET

from authentication.admin_authentication import (authenticate_researcher_login,
    get_researcher_allowed_studies_as_query_set)
from libs.internal_types import ResearcherRequest


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