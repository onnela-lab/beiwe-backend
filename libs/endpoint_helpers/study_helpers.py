from collections import defaultdict
from typing import Any, Dict, List

from django.contrib import messages
from django.core.exceptions import ValidationError
from django.db.models import QuerySet

from constants.message_strings import (ENDED_STUDY_MESSAGE, HIDDEN_STUDY_MESSAGE,
    MANUALLY_STOPPED_STUDY_MESSAGE)
from database.study_models import DeviceSettings, Study
from libs.internal_types import ResearcherRequest


## Utils, really

def get_administerable_studies_by_name(request: ResearcherRequest) -> QuerySet[Study]:
    """ Gets Studies ordered by name. site admins see all studies, study admins see only studies
    they are admins on. """
    if request.session_researcher.site_admin:
        return Study.get_all_studies_by_name()
    else:
        return request.session_researcher.get_administered_studies_by_name()


def conditionally_display_study_status_warnings(request: ResearcherRequest, study: Study):
    """ Display warnings to the user about the status of a study, used on several pages. """
    # deleted means hidden. This weird detail is because we never want to delete an encryption key.
    # some pages simply will not load if the study is deleted.
    if study.deleted:
        messages.warning(request, HIDDEN_STUDY_MESSAGE)
    if study.manually_stopped:
        messages.warning(request, MANUALLY_STOPPED_STUDY_MESSAGE)
    if study.end_date_is_in_the_past:
        messages.warning(request, ENDED_STUDY_MESSAGE.format(study.end_date.isoformat()))


## Update Study Device Settings helpers


def unflatten_consent_sections(consent_sections_dict: dict):
    """ "unflattens" a dictionary of consent sections (study device settings) into a nested structure. """
    # consent_sections is a flat structure with structure like this:
    # { 'label_ending_in.text': 'text content',  'label_ending_in.more': 'more content' }
    # we need to transform it into a nested structure like this:
    # { 'label': {'text':'text content',  'more':'more content' }
    refactored_consent_sections = defaultdict(dict)
    for key, content in consent_sections_dict.items():
        _, label, content_type = key.split(".")
        refactored_consent_sections[label][content_type] = content
    return dict(refactored_consent_sections)


def try_update_device_settings(request: ResearcherRequest, params: Dict[str, Any], study: Study):
    """ Attempts to update, backs off if there were any failures, notifies users of bad fields.
    (finally a situation where django forms would be better, sorta, I don't think it allows partial
    updates without mucking around.) """
    try:
        study.device_settings.update(**params)
    except ValidationError as validation_errors:
        old_device_settings = DeviceSettings.objects.get(study=study)
        
        # ValidationError.message_dict is the least obtuse way to do this
        for field, field_messages in validation_errors.message_dict.items():
            # remove new value from device settings (ugly, whatever)
            setattr(study.device_settings, field, getattr(old_device_settings, field))
            for msg in field_messages:
                messages.error(request, f"{field.replace('_', ' ').title()} wos NOT updated, '{msg}'")
        
        # save without the bad fields
        study.device_settings.save()


def trim_whitespace(request: ResearcherRequest, params: Dict[str, Any], notify: bool = False):
    """ Trims whitespace from all dictionary values, used when updating study device settings. """
    for k, v in params.items():
        if isinstance(v, str):
            v_trimmed = v.strip()
            if v_trimmed != v:
                params[k] = v_trimmed
                if notify:
                    messages.info(request, message=f"whitespace was trimmed on {k.replace('_', ' ')}")


def notify_changes(
    request: ResearcherRequest, params: Dict[str, Any], comparee: Dict[str, Any], message_prefix: str = ""
):
    """ Determines differences between 2 dictionaries and notifies the user based on key name values.
    Used when making changes to study settings. """
    # convert to string to compare value representations (assumes type conversion is already handled)
    updated = [k.replace("_", " ").title() for k, v in params.items() if str(comparee[k]) != str(v)]
    updated.sort()
    if len(updated) == 1:
        messages.info(request, message_prefix + f"{updated[0]} was updated.")
    elif len(updated) > 1:
        start, end = f"{', '.join(updated)} were all updated.".rsplit(",", 1)
        end = ", and" + end
        messages.info(request, message_prefix + start + end)
