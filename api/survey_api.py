from flask import abort, Blueprint, make_response, request, redirect, json

from libs.admin_authentication import authenticate_researcher_study_access
from libs.json_logic import do_validate_survey
from database.study_models import Survey

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

    study_id = survey.study_id
    survey.mark_deleted()
    return redirect('/view_study/{:d}'.format(study_id))

################################################################################
############################# Setters and Editors ##############################
################################################################################


@survey_api.route('/update_survey/<string:survey_id>', methods=['GET', 'POST'])
@authenticate_researcher_study_access
def update_survey(survey_id=None):
    try:
        survey = Survey.objects.get(pk=survey_id)
    except Survey.DoesNotExist:
        return abort(404)

    # BUG: There is an unknown situation where the frontend sends a string requiring an extra
    # deserialization operation, causing 'content' to be a string containing a json string
    # containing a json list, instead of just a string containing a json list.
    # print(request.values.get('content', ''))
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
    
    # These three all stay JSON when added to survey
    content = json.dumps(content)
    timings = request.values['timings']
    settings = request.values['settings']
    survey.update(content=content, timings=timings, settings=settings)
    
    return make_response("", 201)


def recursive_survey_content_json_decode(json_entity):
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
