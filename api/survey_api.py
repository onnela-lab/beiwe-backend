from flask import abort, Blueprint, flash, json, make_response, redirect, request

from authentication.admin_authentication import authenticate_researcher_study_access
from database.schedule_models import AbsoluteSchedule, RelativeSchedule, WeeklySchedule
from database.survey_models import Survey
from libs.json_logic import do_validate_survey
from libs.push_notification_helpers import (repopulate_absolute_survey_schedule_events,
    repopulate_relative_survey_schedule_events, repopulate_weekly_survey_schedule_events)


survey_api = Blueprint('survey_api', __name__)

################################################################################
############################## Creation/Deletion ###############################
################################################################################


@survey_api.route('/create_survey/<string:study_id>/<string:survey_type>', methods=['GET', 'POST'])
@authenticate_researcher_study_access
def create_survey(study_id=None, survey_type='tracking_survey'):
    new_survey = Survey.create_with_settings(study_id=study_id, survey_type=survey_type)
    return redirect('/edit_survey/{:d}'.format(new_survey.id))


@survey_api.route('/delete_survey/<string:survey_id>', methods=['GET', 'POST'])
@authenticate_researcher_study_access
def delete_survey(survey_id=None):
    try:
        survey = Survey.objects.get(pk=survey_id)
    except Survey.DoesNotExist:
        return abort(404)
    # mark as deleted, delete all schedules and schedule events
    survey.deleted = True
    survey.save()
    # clear out any active schedules
    survey.absolute_schedules.all().delete()
    survey.relative_schedules.all().delete()
    survey.weekly_schedules.all().delete()
    return redirect(f'/view_study/{survey.study_id}')

################################################################################
############################# Setters and Editors ##############################
################################################################################


@survey_api.route('/update_survey/<string:survey_id>', methods=['GET', 'POST'])
@authenticate_researcher_study_access
def update_survey(survey_id=None):
    """
    Updates the survey when the 'Save & Deploy button on the edit_survey page is hit. Expects
    content, weekly_timings, absolute_timings, relative_timings, and settings in the request body
    """
    try:
        survey = Survey.objects.get(pk=survey_id)
    except Survey.DoesNotExist:
        return abort(404)
    # BUG: There is an unknown situation where the frontend sends a string requiring an extra
    # deserialization operation, causing 'content' to be a string containing a json string
    # containing a json list, instead of just a string containing a json list.
    json_content = request.values.get('content')
    content = None

    # Weird corner case: the Image survey does not have any content associated with it. Therefore,
    # when you try and make a post request to save any settings you have, it gives you a 500 error
    # because the request.values.get('content') returns a json item of "". The recursive_survey_content_json_decode
    # function is not able to decode 2 double quotations marks. This is why retrieving the json_content from the post
    # request is put outside of the decode statement. HOWEVER, evaluating json_content == "" returns false, since the
    # LITERAL value of the json_content is 2 quotation marks, NOT an empty string. Thus, we need to compare the
    # json_content to a string of 2 quotation marks (ie. '""')
    if json_content != '""':
        content = recursive_survey_content_json_decode(json_content)
        content = make_slider_min_max_values_strings(content)
    if survey.survey_type == Survey.TRACKING_SURVEY:
        errors = do_validate_survey(content)
        if len(errors) > 1:
            return make_response(json.dumps(errors), 400)

    # For each of the schedule types, creates Schedule objects and ScheduledEvent objects
    weekly_timings = json.loads(request.values['weekly_timings'])
    w_duplicated = WeeklySchedule.create_weekly_schedules(weekly_timings, survey)
    repopulate_weekly_survey_schedule_events(survey)
    absolute_timings = json.loads(request.values['absolute_timings'])
    a_duplicated = AbsoluteSchedule.create_absolute_schedules(absolute_timings, survey)
    repopulate_absolute_survey_schedule_events(survey)
    relative_timings = json.loads(request.values['relative_timings'])
    r_duplicated = RelativeSchedule.create_relative_schedules(relative_timings, survey)
    repopulate_relative_survey_schedule_events(survey)

    # These three all stay JSON when added to survey
    content = json.dumps(content)
    settings = request.values['settings']
    survey.update(content=content, settings=settings)

    # if any duplicate schedules were submitted, flash a message
    if w_duplicated or a_duplicated or r_duplicated:
        flash('Duplicate schedule was submitted. Only one of the duplicates was created.', 'success')
    return make_response("", 201)


def recursive_survey_content_json_decode(json_entity: str):
    """ Decodes through up to 100 attempts a json entity until it has deserialized to a list. """
    count = 100
    decoded_json = None
    while not isinstance(decoded_json, list):
        count -= 1
        if count < 0:
            raise Exception("could not decode json entity to list")
        decoded_json = json.loads(json_entity)
    return decoded_json


def make_slider_min_max_values_strings(json_content):
    """ Turns min/max int values into strings, because the iOS app expects strings. This is for
    backwards compatibility; when all the iOS apps involved in studies can handle ints,
    we can remove this function. """
    for question in json_content:
        if 'max' in question:
            question['max'] = str(question['max'])
        if 'min' in question:
            question['min'] = str(question['min'])
    return json_content
