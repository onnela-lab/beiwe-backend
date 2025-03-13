from config.settings import ENABLE_EXPERIMENTS
from constants.user_constants import ANDROID_API, IOS_API
from database.study_models import Study
from libs.internal_types import ResearcherRequest


# NOTE: there is documentation on the django documentation page about using context processors with
# jinja2. search for "Using context processors with Jinja2 templates is discouraged"
# on https://docs.djangoproject.com/en/4.1/topics/templates/
# If you want to add something globally it is probably best to stick it in /config/jinja2.py
# otherwise you will get jinja2.exceptions.UndefinedError errors under otherwise normal conditions


def researcher_context_processor(request: ResearcherRequest):
    # Common assets used on admin pages
    ret = {}
    
    # if it is a researcher endpoint (aka has the admin or researcher or study/survey authentication
    # decorators) then we need most of these variables available in the template.
    if hasattr(request, "session_researcher"):
        # the studies dropdown is on almost all pages.
        allowed_studies_kwargs = {} if request.session_researcher.site_admin else \
            {"study_relations__researcher": request.session_researcher}
        
        allowed_studies = list(
            Study.get_all_studies_by_name().filter(**allowed_studies_kwargs)
                .values("name", "object_id", "id")
        )
        for study in allowed_studies:
            study["search_text"] = f"{study['name']} ({study['object_id']})"
        
        ret["allowed_studies"] = allowed_studies
        ret["is_admin"] = request.session_researcher.is_an_admin()
        ret["site_admin"] = request.session_researcher.site_admin
        ret["session_researcher"] = request.session_researcher
        ret["IOS_API"] = IOS_API
        ret["ANDROID_API"] = ANDROID_API
        ret["ENABLE_EXPERIMENTS"] = ENABLE_EXPERIMENTS
    return ret
