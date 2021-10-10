
from flask import abort, Blueprint, flash, redirect, request, Response

from authentication.admin_authentication import authenticate_admin
from database.study_models import Study
from database.survey_models import Survey
from libs.copy_study import (allowed_file_extension, copy_study_from_json, format_study,
    unpack_json_study)


copy_study_api = Blueprint('copy_study_api', __name__)

"""
JSON structure for exporting and importing study surveys and settings:
    {
     'device_settings': {},
     'surveys': [{}, {}, ...]
    }
    Also, purge all id keys
"""

@copy_study_api.route('/export_study_settings_file/<string:study_id>', methods=['GET'])
@authenticate_admin
def export_study_settings_file(study_id):
    """ Endpoint that returns a json representation of a study. """
    study = Study.objects.get(pk=study_id)
    filename = study.name.replace(' ', '_') + "_surveys_and_settings.json"
    return Response(
        format_study(study),
        mimetype="application/json",
        headers={'Content-Disposition': 'attachment;filename=' + filename}
    )


@copy_study_api.route('/import_study_settings_file/<string:study_id>', methods=['POST'])
@authenticate_admin
def import_study_settings_file(study_id):
    """ Endpoint that takes the output of export_study_settings_file and creates a new study. """
    study = Study.objects.get(pk=study_id)
    file = request.files.get('upload', None)
    if not file:
        abort(400)

    if not allowed_file_extension(file.filename):
        flash("You can only upload .json files!", 'danger')
        return redirect(request.referrer)

    copy_device_settings = request.form.get('device_settings', None) == 'true'
    copy_surveys = request.form.get('surveys', None) == 'true'
    device_settings, surveys, interventions = unpack_json_study(file.read())

    initial_tracking_surveys = study.surveys.filter(survey_type=Survey.TRACKING_SURVEY).count()
    initial_audio_surveys = study.surveys.filter(survey_type=Survey.AUDIO_SURVEY).count()
    # initial_image_surveys = study.objects.filter(survey_type=Survey.IMAGE_SURVEY).count()
    copy_study_from_json(
        study,
        device_settings if copy_device_settings else {},
        surveys if copy_surveys else [],
        interventions,
    )
    end_tracking_surveys = study.surveys.filter(survey_type=Survey.TRACKING_SURVEY).count()
    end_audio_surveys = study.surveys.filter(survey_type=Survey.AUDIO_SURVEY).count()
    # end_image_surveys = study.objects.filter(survey_type=Survey.IMAGE_SURVEY).count()
    flash(
        f"Copied {end_tracking_surveys-initial_tracking_surveys} " +
        f"Surveys and {end_audio_surveys-initial_audio_surveys} Audio Surveys",
        'success',
    )
    if copy_device_settings:
        flash(f"Overwrote {study.name}'s App Settings with custom values.", 'success')
    else:
        flash(f"Did not alter {study.name}'s App Settings.", 'success')
    return redirect('/edit_study/{:s}'.format(study_id))
