from flask import json

from libs.s3 import s3_list_files, s3_retrieve


################################ CSV HANDLER ###################################
def csv_to_dict(csv_string):
    """ Converts a string formatted as a csv into a dictionary with the format
        {Column Name: [list of sequential points] }.
        Data are in their original order, empty entries are dropped. """
    # grab a list of every line in the file, strips off trailing whitespace.
    lines = csv_string.splitlines()
    header_list = lines[0].split(b',')
    list_of_entries = []
    for line in lines[1:]:
        data = line.split(b',')
        # creates a dict of {column name: config point, ...}, strips empty strings.
        list_of_entries.append(
            {header_list[i]: entry for i, entry in enumerate(data) if entry not in ('', b"")}
        )
    return list_of_entries


################################################################################
############################### GRAPH DATA #####################################
################################################################################

def grab_file_names(study_id, survey_id, user_id, number_points):
    """ Takes a list, returns a list of those most recent files."""
    # this is correct - we want to convert these values to strings, not coerce them, that causes them
    # to be converted to strings with a preceeding b and in single quotes.

    study_id = study_id if not isinstance(study_id, bytes) else study_id.decode()
    survey_id = survey_id if not isinstance(survey_id, bytes) else survey_id.decode()
    user_id = user_id if not isinstance(user_id, bytes) else user_id.decode()
    number_points = number_points if not isinstance(number_points, bytes) else number_points.decode()

    all_files = s3_list_files("%s/%s/surveyAnswers/%s" % (str(study_id), str(user_id), str(survey_id)))
    return sorted(all_files[-number_points:])


def compile_question_data(surveys):
    """ creates a double nested dict of question ids containing dict with question text,
    and an empty list. """
    # Note: we want to keep the question text around so it can be displayed on the page.
    if not surveys:
        return {}
    all_questions = {}
    for question in surveys[0]:  # we only need to get the questions once
        all_questions[question['question id']] = {question['question text']: []}
    return all_questions


def pull_answers(surveys, all_questions):
    """ Runs through questions and pull out answers, append them to the lists in the container
    constructed by compile_question_data. """
    for survey in surveys:
        for question in survey:
            question_id = question['question id']
            answer = question['answer']
            question_text = question['question text']
            try:
                all_questions[question_id][question_text].append(int(answer))
            except ValueError:
                all_questions[question_id][question_text].append(None)
    return all_questions


def get_survey_results(study_id, user_id, survey_id, number_points=7):
    """ Compiles 2 weeks (14 points) of config from s3 for a given patient into config points for
    displaying on the device.
    Result is a list of lists, inner list[0] is the title/question text, inner list[1] is a
    list of y coordinates. """
    # Get files from s3 for user answers, convert each csv_file to a list of dicts,
    # pull the questions and corresponding answers.
    files = grab_file_names(study_id, survey_id, user_id, number_points)
    # note: we need to remove the prepended study id from file name strings,
    # that string is always 24 characters + the slash.
    surveys = [csv_to_dict(s3_retrieve(file_name[25:], study_id)) for file_name in files]
    all_questions = compile_question_data(surveys)
    all_answers = pull_answers(surveys, all_questions)
    # all answers may be identical to all questions at this point.
    # turn the data into a list of lists that javascript can actually handle.
    questions_answers = sorted(all_answers.values())  # pulls out question text and answers.
    result = []
    for question in questions_answers:  # maps question answers to data points
        for question_text, corresponding_answers in question.items():
            result.append([question_text, corresponding_answers])
    return jsonify_survey_results(result)


def jsonify_survey_results(results):
    """ Transforms output of get_survey_results into a list that javascript can actually handle. """
    return_data = []
    for pair in results:
        coordinates = [json.dumps(coordinate) for coordinate in pair[1]]
        # javascript understands json null/none values but not python Nones,
        # to handle this we must dump all variables individually
        return_data.append([json.dumps(pair[0]), coordinates])
    return return_data
