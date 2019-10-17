from json import dumps, loads
from flask import Blueprint, flash, redirect, request, Response

from libs.admin_authentication import authenticate_admin
from libs.copy_study import add_new_surveys, allowed_filename, update_device_settings
from database.study_models import Study

copy_study_api = Blueprint('copy_study_api', __name__)

""" JSON structure for exporting and importing study surveys and settings:
    {
     'device_settings': {},
     'surveys': [{}, {}, ...]
    }
    Also, purge all _id keys! """


@copy_study_api.route('/export_study_settings_file/<string:study_id>', methods=['GET'])
@authenticate_admin
def export_study_settings_file(study_id):
    study = Study.objects.get(pk=study_id)
    filename = study.name.replace(' ', '_') + "_surveys_and_settings.json"

    surveys = [s.as_native_python() for s in study.surveys.all()]
    device_settings = study.get_study_device_settings().as_native_python()
    output = {
        'surveys': surveys,
        'device_settings': device_settings,
    }

    return Response(
        dumps(output),
        mimetype="application/json",
        headers={'Content-Disposition': 'attachment;filename=' + filename}
    )


@copy_study_api.route('/import_study_settings_file/<string:study_id>', methods=['POST'])
@authenticate_admin
def import_study_settings_file(study_id):
    study = Study.objects.get(pk=study_id)
    file = request.files['upload']
    if not allowed_filename(file.filename):
        flash("You can only upload .json files!", 'danger')
        return redirect(request.referrer)

    study_data = loads(file.read())
    msg = update_device_settings(study_data['device_settings'], study, file.filename)
    msg += " \n" + add_new_surveys(study_data['surveys'], study, file.filename)
    flash(msg, 'success')
    return redirect('/edit_study/{:s}'.format(study_id))
