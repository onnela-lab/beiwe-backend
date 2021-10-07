from django.core.exceptions import ValidationError
from flask import abort, Blueprint, flash, redirect, request
from flask.templating import render_template

from authentication.admin_authentication import (assert_admin, assert_researcher_under_admin,
    authenticate_admin, authenticate_researcher_login, get_researcher_allowed_studies,
    get_session_researcher, researcher_is_an_admin)
from config.constants import ResearcherRole
from config.settings import DOMAIN_NAME, DOWNLOADABLE_APK_URL
from database.study_models import Study
from database.user_models import Researcher, StudyRelation
from libs.push_notification_helpers import repopulate_all_survey_scheduled_events
from libs.security import check_password_requirements
from libs.timezone_dropdown import ALL_TIMEZONES


admin_api = Blueprint('admin_api', __name__)

"""######################### Study Administration ###########################"""


@admin_api.route('/set_study_timezone/<string:study_id>', methods=['POST'])
@authenticate_admin
def set_timezone(study_id=None):
    """ Sets the custom timezone on a study. """
    new_timezone = request.values.get("new_timezone_name")
    if new_timezone not in ALL_TIMEZONES:
        flash("The timezone chosen does not exist.", 'warning')
        return redirect('/edit_study/{:d}'.format(study_id))

    study = Study.objects.get(pk=study_id)
    study.timezone_name = new_timezone
    study.save()

    # All scheduled events for this study need to be recalculated
    # this causes chaos, relative and absolute surveys will be regenerated if already sent.
    repopulate_all_survey_scheduled_events(study)

    flash(f"Timezone {study.timezone_name} has been applied.", 'warning')
    return redirect(f'/edit_study/{study_id}')


@admin_api.route('/add_researcher_to_study', methods=['POST'])
@authenticate_admin
def add_researcher_to_study():
    researcher_id = request.values['researcher_id']
    study_id = request.values['study_id']
    assert_admin(study_id)
    try:
        StudyRelation.objects.get_or_create(
            study_id=study_id, researcher_id=researcher_id, relationship=ResearcherRole.researcher
        )
    except ValidationError:
        # handle case of the study id + researcher already existing
        pass

    # This gets called by both edit_researcher and edit_study, so the POST request
    # must contain which URL it came from.
    return redirect(request.values['redirect_url'])


@admin_api.route('/remove_researcher_from_study', methods=['POST'])
@authenticate_admin
def remove_researcher_from_study():
    researcher_id = request.values['researcher_id']
    study_id = request.values['study_id']
    try:
        researcher = Researcher.objects.get(pk=researcher_id)
    except Researcher.DoesNotExist:
        return abort(404)
    assert_admin(study_id)
    assert_researcher_under_admin(researcher, study_id)
    StudyRelation.objects.filter(study_id=study_id, researcher_id=researcher_id).delete()
    return redirect(request.values['redirect_url'])


@admin_api.route('/delete_researcher/<string:researcher_id>', methods=['GET', 'POST'])
@authenticate_admin
def delete_researcher(researcher_id):
    # only site admins can delete researchers from the system.
    session_researcher = get_session_researcher()
    if not session_researcher.site_admin:
        return abort(403)

    try:
        researcher = Researcher.objects.get(pk=researcher_id)
    except Researcher.DoesNotExist:
        return abort(404)

    StudyRelation.objects.filter(researcher=researcher).delete()
    researcher.delete()
    return redirect('/manage_researchers')


@admin_api.route('/set_researcher_password', methods=['POST'])
@authenticate_admin
def set_researcher_password():
    researcher = Researcher.objects.get(pk=request.form.get('researcher_id', None))
    assert_researcher_under_admin(researcher)
    new_password = request.form.get('password', '')
    if check_password_requirements(new_password, flash_message=True):
        researcher.set_password(new_password)
    return redirect('/edit_researcher/{:d}'.format(researcher.pk))


@admin_api.route('/rename_study/<string:study_id>', methods=['POST'])
@authenticate_admin
def rename_study(study_id=None):
    study = Study.objects.get(pk=study_id)
    assert_admin(study_id)
    new_study_name = request.form.get('new_study_name', '')
    study.name = new_study_name
    study.save()
    return redirect('/edit_study/{:d}'.format(study.pk))


"""##### Methods responsible for distributing APK file of Android app. #####"""


@admin_api.route("/downloads")
@authenticate_researcher_login
def download_page():
    return render_template(
        "download_landing_page.html",
        is_admin=researcher_is_an_admin(),
        allowed_studies=get_researcher_allowed_studies(),
        domain_name=DOMAIN_NAME,
    )


@admin_api.route("/download")
def download_current():
    return redirect(DOWNLOADABLE_APK_URL)


@admin_api.route("/download_debug")
@authenticate_researcher_login
def download_current_debug():
    return redirect("https://s3.amazonaws.com/beiwe-app-backups/release/Beiwe-debug.apk")


@admin_api.route("/download_beta")
@authenticate_researcher_login
def download_beta():
    return redirect("https://s3.amazonaws.com/beiwe-app-backups/release/Beiwe.apk")


@admin_api.route("/download_beta_debug")
@authenticate_researcher_login
def download_beta_debug():
    return redirect("https://s3.amazonaws.com/beiwe-app-backups/debug/Beiwe-debug.apk")


@admin_api.route("/download_beta_release")
@authenticate_researcher_login
def download_beta_release():
    return redirect("https://s3.amazonaws.com/beiwe-app-backups/release/Beiwe-2.2.3-onnelaLabServer-release.apk")


@admin_api.route("/privacy_policy")
def download_privacy_policy():
    return redirect("https://s3.amazonaws.com/beiwe-app-backups/Beiwe+Data+Privacy+and+Security.pdf")
