from flask.blueprints import Blueprint
from flask.templating import render_template

from authentication.user_authentication import authenticate_user, get_session_participant
from libs.graph_data import get_survey_results

mobile_pages = Blueprint('mobile_pages', __name__)


@mobile_pages.route('/graph', methods=['GET', 'POST'])
@authenticate_user
def fetch_graph():
    """ Fetches the patient's answers to the most recent survey, marked by survey ID. The results
    are dumped into a jinja template and pushed to the device. """
    participant = get_session_participant()
    # See docs in config manipulations for details
    study_object_id = participant.study.object_id
    survey_object_id_set = participant.study.surveys.values_list('object_id', flat=True)
    data = []
    for survey_id in survey_object_id_set:
        data.append(get_survey_results(study_object_id, participant.patient_id, survey_id, 7))
    return render_template("phone_graphs.html", data=data)


#  this is a debugging function, it displays the user graph for a given user without authentication.
# @mobile_pages.route("/fake", methods=["GET"] )
# def fake_survey():
#     patient_id = request.values['patient_id']
#     user = User(patient_id)
#     #see docs in config manipulations for details.
#     study_id = user['study_id']
#     study = Study(study_id)
#     surveys = study['surveys']
#     data = []
#     for survey_id in surveys:
#         data.append( get_survey_results(study_id, patient_id, survey_id, 7) )
#     return render_template("phone_graphs.html", data=data)
