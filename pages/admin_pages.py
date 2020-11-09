from flask import (abort, Blueprint, flash, Markup, redirect, render_template, request,
    session)

from authentication import admin_authentication
from authentication.admin_authentication import (authenticate_researcher_login,
    authenticate_researcher_study_access, get_researcher_allowed_studies,
    get_researcher_allowed_studies_as_query_set, get_session_researcher, researcher_is_an_admin,
    SESSION_NAME)
from config.settings import PUSH_NOTIFICATIONS_ENABLED
from database.study_models import Study, StudyField
from database.user_models import Participant, ParticipantFieldValue, Researcher
from libs.security import check_password_requirements

admin_pages = Blueprint('admin_pages', __name__)


@admin_pages.context_processor
def inject_html_params():
    # these variables will be accessible to every template rendering attached to the blueprint
    return {
        "allowed_studies": get_researcher_allowed_studies(),
        "is_admin": researcher_is_an_admin(),
    }


@admin_pages.route('/choose_study', methods=['GET'])
@authenticate_researcher_login
def choose_study():
    allowed_studies = get_researcher_allowed_studies_as_query_set()

    # If the admin is authorized to view exactly 1 study, redirect to that study
    if allowed_studies.count() == 1:
        return redirect('/view_study/{:d}'.format(allowed_studies.values_list('pk', flat=True).get()))

    # Otherwise, show the "Choose Study" page

    return render_template(
        'choose_study.html',
        studies=[obj.as_unpacked_native_python() for obj in allowed_studies],
        allowed_studies=get_researcher_allowed_studies(),
        is_admin=researcher_is_an_admin()
    )


@admin_pages.route('/view_study/<string:study_id>', methods=['GET'])
@authenticate_researcher_study_access
def view_study(study_id=None):
    study = Study.objects.get(pk=study_id)
    researcher = get_session_researcher()
    participants = study.participants.all()

    # creates dicts of Custom Fields and Interventions to be easily accessed in the template
    for p in participants:
        p.field_dict = participant_tags(p)
        p.intervention_dict = {tag.intervention.name: tag.date for tag in p.intervention_dates.all()}

    return render_template(
        'view_study.html',
        study=study,
        participants=participants,
        audio_survey_ids=study.get_survey_ids_and_object_ids('audio_survey'),
        image_survey_ids=study.get_survey_ids_and_object_ids('image_survey'),
        tracking_survey_ids=study.get_survey_ids_and_object_ids('tracking_survey'),
        # these need to be lists because they will be converted to json.
        study_fields=list(study.fields.all().values_list('field_name', flat=True)),
        interventions=list(study.interventions.all().values_list("name", flat=True)),
        page_location='study_landing',
        study_id=study_id,
        readonly=not researcher.check_study_admin(study_id) and not researcher.site_admin,
        push_notifications_enabled=PUSH_NOTIFICATIONS_ENABLED,
    )


@admin_pages.route('/data-pipeline/<string:study_id>', methods=['GET'])
@authenticate_researcher_study_access
def view_study_data_pipeline(study_id=None):
    return render_template('data-pipeline.html', study=Study.objects.get(pk=study_id))


# TODO: delete, this end point cannot be hit, this page isn't used anymore, can delete template too
@admin_pages.route('/view_study/<string:study_id>/patient_fields/<string:patient_id>', methods=['GET', 'POST'])
@authenticate_researcher_study_access
def patient_fields(study_id, patient_id=None):
    # the study is already authenticated at this time,
    try:
        participant = Participant.objects.get(pk=int(patient_id))  # int coercion is sanitization
        study = participant.study
    except Participant.DoesNotExist:
        return abort(404)

    # safety chack
    if study.id != study_id:
        return abort(403)

    participant.values_dict = participant_tags(participant)
    if request.method == 'GET':
        return render_template(
            'view_patient_custom_field_values.html',
            fields=study.fields.all(),
            study=study,
            patient=participant,
        )

    # todo: sanitize.
    fields = list(study.fields.values_list('field_name', flat=True))
    for key, value in request.values.items():
        if key in fields:
            pfv, created = ParticipantFieldValue.objects.get_or_create(
                participant=participant, field=StudyField.objects.get(study=study, field_name=key)
            )
            pfv.value = value
            pfv.save()

    return redirect('/view_study/{:d}'.format(study.id))


"""########################## Login/Logoff ##################################"""


@admin_pages.route("/logout")
def logout():
    admin_authentication.logout_researcher()
    return redirect("/")




@admin_pages.route('/manage_credentials')
@authenticate_researcher_login
def manage_credentials():
    return render_template('manage_credentials.html')


@admin_pages.route('/reset_admin_password', methods=['POST'])
@authenticate_researcher_login
def reset_admin_password():
    username = session[SESSION_NAME]
    current_password = request.values['current_password']
    new_password = request.values['new_password']
    confirm_new_password = request.values['confirm_new_password']

    if not Researcher.check_password(username, current_password):
        flash("The Current Password you have entered is invalid", 'danger')
        return redirect('/manage_credentials')
    if not check_password_requirements(new_password, flash_message=True):
        return redirect("/manage_credentials")
    if new_password != confirm_new_password:
        flash("New Password does not match Confirm New Password", 'danger')
        return redirect('/manage_credentials')

    # todo: sanitize password?
    Researcher.objects.get(username=username).set_password(new_password)
    flash("Your password has been reset!", 'success')
    return redirect('/manage_credentials')


@admin_pages.route('/reset_download_api_credentials', methods=['POST'])
@authenticate_researcher_login
def reset_download_api_credentials():
    researcher = Researcher.objects.get(username=session[SESSION_NAME])
    access_key, secret_key = researcher.reset_access_credentials()

    msg = """<h3>Your Data-Download API access credentials have been reset!</h3>
        <p>Your new <b>Access Key</b> is:
          <div class="container-fluid">
            <textarea rows="1" cols="85" readonly="readonly" onclick="this.focus();this.select()">%s</textarea></p>
          </div>
        <p>Your new <b>Secret Key</b> is:
          <div class="container-fluid">
            <textarea rows="1" cols="85" readonly="readonly" onclick="this.focus();this.select()">%s</textarea></p>
          </div>
        <p>Please record these somewhere; they will not be shown again!</p>""" \
        % (access_key, secret_key)
    flash(Markup(msg), 'warning')
    return redirect("/manage_credentials")



@admin_pages.route('/upload_firebase_cert', methods=['POST'])
@authenticate_researcher_login
def upload_firebase_cert():
    researcher = Researcher.objects.get(username=session[SESSION_NAME])
    v = request.files['fileToUpload']
    researcher.firebase_cert = str(v.read(), 'utf-8')
    researcher.save()
    return redirect("/manage_credentials")

def participant_tags(p: Participant):
        return {tag.field.field_name: tag.value for tag in p.field_values.all()}
