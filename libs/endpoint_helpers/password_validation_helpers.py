import re
from typing import Tuple

from constants.message_strings import NEW_PASSWORD_N_LONG, NEW_PASSWORD_RULES_FAIL
from constants.security_constants import PASSWORD_REQUIREMENT_REGEX_LIST
from database.user_models_researcher import Researcher


def check_password_requirements(researcher: Researcher, password: str) -> Tuple[bool, str]:
    """ Runs all the password requirement tests for researcher passwords. """
    researcher_min = get_min_password_requirement(researcher)
    if len(password) < researcher_min:
        return False, NEW_PASSWORD_N_LONG.format(length=researcher_min)
    
    for regex in PASSWORD_REQUIREMENT_REGEX_LIST:
        if not re.search(regex, password):
            return False, NEW_PASSWORD_RULES_FAIL
    
    return True, None


def get_min_password_requirement(researcher: Researcher) -> int:
    """ Returns the minimum required password length for a researcher. """
    # studies define their own minimum
    min_length = researcher.study_relations.order_by("-study__password_minimum_length") \
        .values_list("study__password_minimum_length", flat=True).first() or 0
    # for site admins it is 20
    if researcher.site_admin:
        min_length = 20
    # the absolute minimum is 8
    return max(min_length, 8)