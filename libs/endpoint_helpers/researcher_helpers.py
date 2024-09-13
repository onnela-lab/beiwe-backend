from typing import List

from django.db.models import F, Func

from database.user_models_researcher import Researcher
from libs.internal_types import ResearcherRequest


def get_administerable_researchers(request: ResearcherRequest) -> List[Researcher]:
    """ Site admins see all researchers, study admins see researchers on their studies. """
    if request.session_researcher.site_admin:
        return Researcher.filter_alphabetical()
    else:
        return request.session_researcher.get_administered_researchers() \
                .annotate(username_lower=Func(F('username'), function='LOWER')) \
                .order_by('username_lower')