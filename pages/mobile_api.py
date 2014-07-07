from flask import Blueprint, request, abort, jsonify, json
from werkzeug import secure_filename
from utils.s3 import s3_upload_handler

mobile_api = Blueprint('mobile_api', __name__)

ALLOWED_EXTENSIONS = set(['csv', 'mp3'])

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

def _upload(file_obj):
    if file_obj and allowed_file(file_obj.filename):
        s3_upload_handler(secure_filename(file_obj.filename), file_obj)

@mobile_api.route('/fetch_survey/', methods=['GET', 'POST'])
def fetch_survey():
    f = open("/var/www/scrubs/sample_survey.json", 'rb')
    return jsonify(json.load(f))
    if request.method == 'POST':
        if request.values["magic"] == "12345":
            return json.load("/var/www/scrubs/sample_survey.json")
    else:
        return

@mobile_api.route('/upload_gps/', methods=['GET', 'POST'])
def upload_gps():
    if request.method == 'POST' and request.files['file']:
        _upload(request.files['file'])
        #mongo_instance.save()
        return'200'
    else:
        abort(400)

@mobile_api.route('/upload_accel/', methods=['GET', 'POST'])
def upload_accel():
    if request.method == 'POST' and request.files['file']:
        _upload(request.files['file'])
        #mongo_instance.save()
        return'200'
    else:
        abort(400)

@mobile_api.route('/upload_powerstate/', methods=['GET', 'POST'])
def upload_powerstate():
    if request.method == 'POST' and request.files['file']:
        _upload(request.files['file'])
        #mongo_instance.save()
        return'200'
    else:
        abort(400)

@mobile_api.route('/upload_calls/', methods=['GET', 'POST'])
def upload_calls():
    if request.method == 'POST' and request.files['file']:
        _upload(request.files['file'])
        #mongo_instance.save()
        return'200'
    else:
        abort(400)

@mobile_api.route('/upload_texts/', methods=['GET', 'POST'])
def upload_texts():
    if request.method == 'POST' and request.files['file']:
        _upload(request.files['file'])
        #mongo_instance.save()
        return'200'
    else:
        abort(400)

@mobile_api.route('/upload_surveyresponse/', methods=['GET', 'POST'])
def upload_surveyresponse():
    if request.method == 'POST' and request.files['file']:
        _upload(request.files['file'])
        #mongo_instance.save()
        return'200'
    else:
        abort(400)

@mobile_api.route('/upload_audio/', methods=['GET', 'POST'])
def upload_audio():
    if request.method == 'POST' and request.files['file']:
        _upload(request.files['file'])
        #mongo_instance.save()
        return'200'
    else:
        abort(400)
