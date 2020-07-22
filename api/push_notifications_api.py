import json
from datetime import datetime

from firebase_admin import messaging
from flask import Blueprint, request

from config import constants
from authentication.user_authentication import authenticate_user, get_session_participant

push_notifications_api = Blueprint('push_notifications_api', __name__)


################################################################################
########################### NOTIFICATION FUNCTIONS #############################
################################################################################


@push_notifications_api.route('/set_fcm_token', methods=['POST'])
@authenticate_user
def set_fcm_token():
    """
    Sets a participants Firebase CLoud Messaging (FCM) instance token, called whenever a new token
    is generated. Expects a patient_id and and fcm_token in the request body.
    """
    participant = get_session_participant()
    fcm_token = participant.get_fcm_token()
    fcm_token.token = request.values['fcm_token']
    fcm_token.save()
    # print("Patient", participant.patient_id, "token: ", request.values['fcm_token'])
    return '', 204


@push_notifications_api.route('/test_notification', methods=['POST'])
@authenticate_user
def send_notification():
    """
    Sends a push notification to the participant, used for testing
    Expects a patient_id in the request body.
    """
    message = messaging.Message(
        data={
            'type': 'fake',
            'content': 'hello good sir',
        },
        token=get_session_participant().get_fcm_token().token,
    )
    response = messaging.send(message)
    print('Successfully sent notification message:', response)
    return '', 204


@push_notifications_api.route('/send_survey_notification', methods=['Post'])
@authenticate_user
def send_survey_notification():
    """
    Sends a push notification to the participant with survey data, used for testing
    Expects a patient_id in the request body
    """
    participant = get_session_participant()
    survey_ids = [  # up to 4 random, valid surveys.
        survey.object_id for survey in participant.study.surveys.filter(deleted=False).order_by("?")[:4]
    ]
    survey_ids.sort()
    sent_time = datetime.now().strftime(constants.API_TIME_FORMAT)
    message = messaging.Message(
        data={
            'type': 'survey',
            'survey_ids': json.dumps(survey_ids),
            'sent_time': sent_time,
        },
        token=participant.get_fcm_token().token,
    )
    response = messaging.send(message)
    print('Successfully sent survey message:', response)
    return '', 204

