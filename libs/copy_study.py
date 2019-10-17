import json
from os import path

from flask import flash, request

from database.study_models import Study, Survey


def copy_existing_study_if_asked_to(new_study):
    if request.form.get('copy_existing_study', 'false') == 'true':
        old_study = Study.objects.get(pk=request.form.get('existing_study_id', None))
        old_device_settings = old_study.device_settings.as_dict()
        old_device_settings.pop('study')
        msg = update_device_settings(old_device_settings, new_study, old_study.name)
        
        surveys_to_copy = []
        for survey in old_study.surveys.all():
            survey_as_dict = survey.as_dict()
            survey_as_dict.pop('study')
            survey_as_dict.pop('created_on')
            surveys_to_copy.append(survey_as_dict)
        msg += " \n" + add_new_surveys(surveys_to_copy, new_study, old_study.name)
        flash(msg, 'success')


def allowed_filename(filename):
    """ Does filename end with ".json" (case-insensitive) """
    return path.splitext(filename)[1].lower() == '.json'


def update_device_settings(new_device_settings, study, filename):
    """ Takes the provided loaded json serialization of a study's device settings and
    updates the provided study's device settings.  Handles the cases of different legacy
    serialization of the consent_sections parameter. """
    if request.form.get('device_settings', 'false') == 'true':
        # Don't copy the PK to the device settings to be updated
        if 'id' in new_device_settings:
            new_device_settings.pop('id')
        if '_id' in new_device_settings:
            new_device_settings.pop('_id')
        if 'created_on' in new_device_settings:
            new_device_settings.pop('created_on')
        
        # ah, it looks like the bug we had was that you can just send dictionary directly
        # into a textfield and it uses the __repr__ or __str__ or __unicode__ function, causing
        # weirdnesses if as_native_python is called because json does not want to use double quotes.
        if isinstance(new_device_settings['consent_sections'], dict):
            new_device_settings['consent_sections'] = json.dumps(new_device_settings['consent_sections'])
        
        study.device_settings.update(**new_device_settings)
        return "Overwrote %s's App Settings with the values from %s." % \
               (study.name, filename)
    else:
        return "Did not alter %s's App Settings." % study.name


def add_new_surveys(new_survey_settings, study, filename):
    surveys_added = 0
    audio_surveys_added = 0
    if request.form.get('surveys', 'false') == 'true':
        for survey_settings in new_survey_settings:

            # Don't copy unique fields to the new Survey object
            unique_fields = ['id', 'object_id', '_id']
            for field in unique_fields:
                if field in survey_settings:
                    survey_settings.pop(field)

            survey_settings['content'] = json.dumps(survey_settings['content'])
            survey_settings['settings'] = json.dumps(survey_settings['settings'])

            Survey.create_with_object_id(study=study, **survey_settings)
            if survey_settings['survey_type'] == 'tracking_survey':
                surveys_added += 1
            elif survey_settings['survey_type'] == 'audio_survey':
                audio_surveys_added += 1

    return "Copied %i Surveys and %i Audio Surveys from %s to %s." % \
           (surveys_added, audio_surveys_added, filename, study.name)
