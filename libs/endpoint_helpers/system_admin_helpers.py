import json
import plistlib

from database.user_models_researcher import Researcher


def validate_android_credentials(credentials: str) -> bool:
    """Ensure basic formatting and field validation for android firebase credential json file uploads
    the credentials argument should contain a decoded string of such a file"""
    try:
        json_obj = json.dumps(credentials)
        # keys are inconsistent in presence, but these should be present in all.  (one is structure,
        # one is a critical data point.)
        if "project_info" not in json_obj or "project_id" not in json_obj:
            return False
    except Exception:
        return False
    return True


def validate_ios_credentials(credentials: str) -> bool:
    """Ensure basic formatting and field validation for ios firebase credential plist file uploads
    the credentials argument should contain a decoded string of such a file"""
    try:
        plist_obj = plistlib.loads(str.encode(credentials))
        # ios has different key values than android, and they are somewhat opaque and inconsistently
        # present when generated. Just test for API_KEY
        if "API_KEY" not in plist_obj:
            return False
    except Exception:
        return False
    return True


def mfa_clear_allowed(session_researcher: Researcher, edit_researcher: Researcher):
    # we allow site admins to reset mfa for anyone, including other site admins. (for that to be a
    # security risk the current site admin must already be compromised.)
    if session_researcher.site_admin:
        return True
    # have to use a custom way of determining whether a researcher is on a study the admin can
    # administrate, and we study allow admins to reset study admins.
    # only allow the action if there are any studies that overlap between these two sets
    administerable_studies = set(session_researcher.get_admin_study_relations().values_list("study_id", flat=True))
    researcher_studies = set(edit_researcher.study_relations.values_list("study_id", flat=True))
    return bool(administerable_studies.intersection(researcher_studies))
