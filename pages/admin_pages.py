
import datetime
from collections import defaultdict

from flask import (abort, Blueprint, flash, Markup, redirect, render_template, request, session,
    url_for)

from authentication import admin_authentication
from authentication.admin_authentication import (authenticate_researcher_login,
    authenticate_researcher_study_access, get_researcher_allowed_studies,
    get_researcher_allowed_studies_as_query_set, get_session_researcher, researcher_is_an_admin,
    SESSION_NAME)
from constants.admin_pages import (DisableApiKeyForm, NEW_API_KEY_MESSAGE, NewApiKeyForm,
    RESET_DOWNLOAD_API_CREDENTIALS_MESSAGE)
from database.data_access_models import ChunkRegistry
from database.security_models import ApiKey
from database.study_models import Study
from database.tableau_api_models import ForestTracker
from database.user_models import Participant, Researcher
from libs.forest_integration.constants import TREES
from libs.push_notification_config import check_firebase_instance
from libs.security import check_password_requirements
from libs.serilalizers import ApiKeySerializer

admin_pages = Blueprint('admin_pages', __name__)

####################################################################################################
############################################# Basics ###############################################
####################################################################################################

@admin_pages.context_processor
def inject_html_params():
    # these variables will be accessible to every template rendering attached to the blueprint
    return {
        "allowed_studies": get_researcher_allowed_studies(),
        "is_admin": researcher_is_an_admin(),
    }


@admin_pages.route("/logout")
def logout():
    admin_authentication.logout_researcher()
    return redirect("/")

####################################################################################################
###################################### Endpoints ###################################################
####################################################################################################


@admin_pages.route('/choose_study', methods=['GET'])
@authenticate_researcher_login
def choose_study():
    allowed_studies = get_researcher_allowed_studies_as_query_set()

    # If the admin is authorized to view exactly 1 study, redirect to that study,
    # Otherwise, show the "Choose Study" page
    if allowed_studies.count() == 1:
        return redirect('/view_study/{:d}'.format(allowed_studies.values_list('pk', flat=True).get()))

    return render_template(
        'choose_study.html',
        studies=[obj.as_unpacked_native_python() for obj in allowed_studies],
        is_admin=researcher_is_an_admin()
    )


@admin_pages.route('/view_study/<string:study_id>', methods=['GET'])
@authenticate_researcher_study_access
def view_study(study_id=None):
    study = Study.objects.get(pk=study_id)
    participants = study.participants.all()

    # creates dicts of Custom Fields and Interventions to be easily accessed in the template
    for p in participants:
        p.field_dict = {tag.field.field_name: tag.value for tag in p.field_values.all()}
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
        is_site_admin=get_session_researcher().site_admin,
        push_notifications_enabled=check_firebase_instance(require_android=True) or
                                   check_firebase_instance(require_ios=True),
    )


@admin_pages.route('/manage_credentials')
@authenticate_researcher_login
def manage_credentials():
    serializer = ApiKeySerializer(ApiKey.objects.filter(researcher=get_session_researcher()), many=True)
    return render_template(
        'manage_credentials.html',
        is_admin=researcher_is_an_admin(),
        api_keys=sorted(serializer.data, reverse=True, key=lambda x: x['created_on']),
    )


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
    flash(Markup(RESET_DOWNLOAD_API_CREDENTIALS_MESSAGE % (access_key, secret_key)), 'warning')
    return redirect("/manage_credentials")


@admin_pages.route('/new_api_key', methods=['POST'])
@authenticate_researcher_login
def new_api_key():
    form = NewApiKeyForm(request.values)
    if not form.is_valid():
        return redirect(url_for("admin_pages.manage_credentials"))
    researcher = Researcher.objects.get(username=session[SESSION_NAME])

    api_key = ApiKey.generate(
        researcher=researcher,
        has_tableau_api_permissions=form.cleaned_data['tableau_api_permission'],
        readable_name=form.cleaned_data['readable_name'],
    )
    # noinspection StrFormat
    msg = NEW_API_KEY_MESSAGE % (api_key.access_key_id, api_key.access_key_secret_plaintext)
    flash(Markup(msg), 'warning')
    return redirect(url_for("admin_pages.manage_credentials"))


@admin_pages.route('/disable_api_key', methods=['POST'])
@authenticate_researcher_login
def disable_api_key():
    form = DisableApiKeyForm(request.values)
    if not form.is_valid():
        return redirect("/manage_credentials")

    api_key_id = request.values["api_key_id"]
    api_key_query = ApiKey.objects.filter(access_key_id=api_key_id).filter(researcher=get_session_researcher())
    if not api_key_query.exists():
        flash(Markup("No matching API key found to disable"), 'warning')
        return redirect("/manage_credentials")
    api_key = api_key_query[0]
    if not api_key.is_active:
        flash("That API key has already been disabled: %s" % api_key_id, 'warning')
        return redirect("/manage_credentials")
    api_key.is_active = False
    api_key.save()
    # flash("The API key %s is now disabled" % str(api_key.access_key_id), 'warning')
    return redirect("/manage_credentials")


@admin_pages.route('/forest_status/<string:study_id>', methods=['GET', 'POST'])
@authenticate_researcher_study_access
def forest_status(study_id=None):
    study = Study.objects.get(pk=study_id)
    if request.method == 'GET':
        return render_template(
            'forest_status.html',
            study=study,
            is_site_admin=get_session_researcher().site_admin,
            status_choices=ForestTracker.Status,
            forest_log=list(ForestTracker.objects.all().order_by("-created_on").values("participant", "forest_tree", "data_date_start", "data_date_end", "status", "external_id", "created_on"))
        )

    # post request is to cancel a forest task, requires site admin permissions
    if not get_session_researcher().site_admin:
        return abort(403)

    try:
        forest_task_id = request.values.get("task_id")
        if forest_task_id is None:
            return abort(404)
        forest_tracker = ForestTracker.objects.get(external_id=forest_task_id)
    except Exception:
        return abort(404)

    forest_tracker.status = ForestTracker.Status.CANCELLED
    forest_tracker.stacktrace = f'canceled by {get_session_researcher().username} on {datetime.date.today()}'
    forest_tracker.save()

    return redirect('/forest_status/{:d}'.format(forest_tracker.participant.study.id))


# source: https://stackoverflow.com/questions/1060279/iterating-through-a-range-of-dates-in-python
def daterange(start, stop, step=datetime.timedelta(days=1), inclusive=False):
    if step.days > 0:
        while start < stop:
            yield start
            start = start + step
            # not +=! don't modify object passed in if it's mutable
            # since this function is not restricted to
            # only types from datetime module
    elif step.days < 0:
        while start > stop:
            yield start
            start = start + step
    if inclusive and start == stop:
        yield start



@admin_pages.route('/study_analysis_progress/<string:study_id>', methods=['GET'])
@authenticate_researcher_study_access
def study_analysis_progress(study_id=None):
    study = Study.objects.get(pk=study_id)
    participants = Participant.objects.filter(study=study_id)

    # generate chart of study analysis progress logs
    trackers = ForestTracker.objects.filter(participant__in=participants).order_by("created_on")

    try:
        start_date = ChunkRegistry.objects.filter(participant__in=participants).earliest("time_bin")
        end_date = ChunkRegistry.objects.filter(participant__in=participants).latest("time_bin")
        start_date = start_date.time_bin.date()
        end_date = end_date.time_bin.date()
    except ChunkRegistry.DoesNotExist:
        start_date = study.created_on.date()
        end_date = datetime.date.today()

    # generate the date range for charting
    dates = list(daterange(start_date, end_date, inclusive=True))

    chart_columns = ["participant", "tree"] + dates
    chart = []

    # this code simultaneously builds up the chart of most recent forest results for date ranges
    # by participant and tree, and tracks the metadata
    metadata = dict()
    results = defaultdict(lambda: "None")
    for tracker in trackers:
        for date in daterange(tracker.data_date_start, tracker.data_date_end, inclusive=True):
            results[(tracker.participant, tracker.forest_tree, date)] = tracker.status
            if tracker.status == tracker.Status.SUCCESS:
                metadata[(tracker.participant, tracker.forest_tree, date)] = tracker.metadata_hash
            else:
                metadata[(tracker.participant, tracker.forest_tree, date)] = None

    for participant in participants:
        for tree in TREES:
            row = [participant.id, tree] + [results[(participant, tree, date)] for date in dates]
            chart.append(row)

    metadata_conflict = False
    # ensure that within each tree, only a single metadata value is used (only the most recent runs
    # are considered, and unsuccessful runs are assumed to invalidate old runs, clearing metadata)
    for tree in set([k[1] for k in metadata.keys()]):
        if len(set([m for k, m in metadata.items() if m is not None and k[1] == tree])) > 1:
            metadata_conflict = True
            break



    return render_template(
        'study_analysis_progress.html',
        study=study,
        is_admin=researcher_is_an_admin(),
        chart_columns=chart_columns,
        status_choices=ForestTracker.Status,
        metadata_conflict=metadata_conflict,
        start_date=start_date,
        end_date=end_date,
        chart=chart  # this uses the jinja safe filter and should never involve user input
    )
