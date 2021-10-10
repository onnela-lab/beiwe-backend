import json
from os import path
from typing import Dict, List, Union

from constants.copy_study_constants import (ABSOLUTE_SCHEDULE_KEY, DEVICE_SETTINGS_KEY,
    INTERVENTIONS_KEY, NEVER_EXPORT_THESE, RELATIVE_SCHEDULE_KEY, STUDY_KEY, SURVEY_CONTENT_KEY,
    SURVEYS_KEY, WEEKLY_SCHEDULE_KEY)
from database.common_models import JSONTextField
from database.schedule_models import (AbsoluteSchedule, Intervention, RelativeSchedule,
    WeeklySchedule)
from database.study_models import Study
from database.survey_models import Survey
from libs.push_notification_helpers import repopulate_all_survey_scheduled_events


NoneType = type(None)


def unpack_json_study(json_string: str) -> Union[dict, List[str], List[dict]]:
    """ Deserializes the data structure of a serialized study """
    study_settings = json.loads(json_string)
    device_settings = study_settings.pop(DEVICE_SETTINGS_KEY, {})
    surveys = study_settings.pop(SURVEYS_KEY, [])
    interventions = study_settings.pop(INTERVENTIONS_KEY, [])
    return device_settings, surveys, interventions


def format_study(study: Study) -> str:
    """ Serializes a study, including surveys, their schedules, device settings, and interventions. """
    device_settings = study.device_settings.as_unpacked_native_python()
    purge_unnecessary_fields(device_settings)
    return json.dumps(
        {
            SURVEYS_KEY: format_surveys(study),
            DEVICE_SETTINGS_KEY: device_settings,
            INTERVENTIONS_KEY: list(study.interventions.values_list("name", flat=True))
        }
    )


def format_surveys(study: Study) -> List[dict]:
    """ Serializes a survey and its schedule. """
    surveys = []
    for survey in study.surveys.filter(deleted=False):
        # content, cleanup, then schedules.
        survey_content = survey.as_unpacked_native_python()
        purge_unnecessary_fields(survey_content)
        survey_content[WEEKLY_SCHEDULE_KEY] = survey.weekly_timings()
        survey_content[ABSOLUTE_SCHEDULE_KEY] = survey.absolute_timings()
        survey_content[RELATIVE_SCHEDULE_KEY] = survey.relative_timings_by_name()
        surveys.append(survey_content)
    return surveys


def purge_unnecessary_fields(d: dict):
    """ removes fields that we don't want re-imported, does so silently. """
    for field in NEVER_EXPORT_THESE:
        d.pop(field, None)


def allowed_file_extension(filename: str):
    """ Does filename end with ".json" (case-insensitive) """
    assert isinstance(filename, str)  # python3, could be a bytes, the line below would always fail.
    return path.splitext(filename)[1].lower() == '.json'


def copy_study_from_json(
    new_study: Study, old_device_settings: dict, surveys_to_copy: List[dict], interventions: List[str]
):
    """ Takes the JSON-deserialized data structures (from unpack_json_study) and creates all
    underlying database structures and relations. """
    if old_device_settings:
        if STUDY_KEY in old_device_settings:
            old_device_settings.pop(STUDY_KEY)
        update_device_settings(old_device_settings, new_study)

    if interventions:
        # The "key" value for intervention on relative survey schedule exports is the name,
        # we can't have duplicate Interventions.  Behavior is that they get merged.
        extant_interventions = set(new_study.interventions.values_list("name", flat=True))
        Intervention.objects.bulk_create(
            [Intervention(name=name, study=new_study)
                for name in interventions if name not in extant_interventions]
        )

    if surveys_to_copy:
        add_new_surveys(new_study, surveys_to_copy)


def update_device_settings(new_device_settings: dict, study: Study):
    """ Takes the provided loaded json serialization of a study's device settings and
    updates the provided study's device settings.  Handles the cases of different legacy
    serialization of the consent_sections parameter. """
    purge_unnecessary_fields(new_device_settings)

    # ah, it looks like the bug we had was that you can just send dictionary directly
    # into a textfield and it uses the __repr__ or __str__ or __unicode__ function, causing
    # weirdnesses if as_unpacked_native_python is called because json does not want to use double quotes.
    if isinstance(new_device_settings['consent_sections'], dict):
        new_device_settings['consent_sections'] = json.dumps(new_device_settings['consent_sections'])
    study.device_settings.update(**new_device_settings)


def schedules_bug_type_check(weekly_schedules, absolute_schedules, relative_schedules):
    # The codepoth code accepts json input, and there was a bug  involving typing once.
    # (this is almost definitely unnecessary.)
    assert isinstance(weekly_schedules, (list, NoneType)), f"weekly_schedule was a {type(weekly_schedules)}."
    assert isinstance(absolute_schedules, (list, NoneType)), f"absolute_schedule was a {type(absolute_schedules)}."
    assert isinstance(relative_schedules, (list, NoneType)), f"relative_schedule was a {type(relative_schedules)}."


def add_new_surveys(study: Study, new_survey_settings: List[Dict]):
    for survey_settings in new_survey_settings:
        # clean out the keys we don't want/need and pop the schedules.
        purge_unnecessary_fields(survey_settings)
        weekly_schedules = survey_settings.pop(WEEKLY_SCHEDULE_KEY, None)
        absolute_schedules = survey_settings.pop(ABSOLUTE_SCHEDULE_KEY, None)
        relative_schedules = survey_settings.pop(RELATIVE_SCHEDULE_KEY, None)
        schedules_bug_type_check(weekly_schedules, absolute_schedules, relative_schedules)

        # convert JSONTextFields to json
        for field in Survey._meta.fields:
            if isinstance(field, JSONTextField):
                survey_settings[field.name] = json.dumps(survey_settings[field.name])

        # case: due to serialization problems (since fixed in a migration) we need to test
        # for this particular scenario and replace a javascript null / Python None with a default.
        if survey_settings[SURVEY_CONTENT_KEY] == "null":
            survey_settings[SURVEY_CONTENT_KEY] = Survey._meta.get_field(SURVEY_CONTENT_KEY).default

        # create survey, schedules, schedule events.
        survey = Survey.create_with_object_id(study=study, **survey_settings)
        AbsoluteSchedule.create_absolute_schedules(absolute_schedules, survey)
        WeeklySchedule.create_weekly_schedules(weekly_schedules, survey)
        create_relative_schedules_by_name(relative_schedules, survey)

        # and if the context is adding surveys to an existing study this must execute.
        repopulate_all_survey_scheduled_events(study)


def create_relative_schedules_by_name(timings: List[List[int]], survey: Survey) -> bool:
    """ This function is based off RelativeSchedule.create_relative_schedules, but contains special
    casing to maintain forwards compatibility with data exported from older versions of Beiwe. """
    survey.relative_schedules.all().delete()  # should always be empty
    if survey.deleted or not timings:
        return

    # Older versions of beiwe failed to export interventions and instead provide integer database
    # keys.  We can detect this case because json preserves the typing of numerics.  You can check
    # the prior commit for a method to identify possible cases where we would populate existing
    # data, but it turns out there aren't generally enough interventions per study to do that.
    if isinstance(timings[0][0], int):
        interventions_lookup = {}
        # count the distinct number of keys, use pks for the lookup dict key
        for i, pk in enumerate({pk for pk, _, _ in timings}):
            interventions_lookup[pk] = Intervention(name=f"Intervention {i}", study=survey.study)
            interventions_lookup[pk].save()
    else:
        # The normal case, where the interventions were present in the json.
        interventions_lookup = {
            intervention.name: intervention
            for intervention in Intervention.objects.filter(study=survey.study)
        }
    for intervention_name, days_after, num_seconds in timings:
        # should be all ints, use integer division.
        RelativeSchedule.objects.create(
            survey=survey,
            intervention=interventions_lookup[intervention_name],
            days_after=days_after,
            hour=num_seconds // 3600,
            minute=num_seconds % 3600 // 60,
        )
