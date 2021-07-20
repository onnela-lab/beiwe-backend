import csv
import datetime
from collections import defaultdict

from django import forms
from django.utils import timezone
from flask import render_template, request, abort, flash, redirect, Blueprint, url_for, Response
from werkzeug.datastructures import MultiDict

from api.data_access_api import chunk_fields
from authentication.admin_authentication import authenticate_researcher_study_access, \
    researcher_is_an_admin, get_session_researcher, authenticate_admin, \
    get_researcher_allowed_studies, forest_enabled
from database.data_access_models import ChunkRegistry
from database.study_models import Study
from database.tableau_api_models import ForestTask
from database.user_models import Participant
from libs.forest_integration.constants import ForestTree
from libs.serializers import ForestTaskSerializer, ForestTaskCsvSerializer
from libs.streaming_zip import zip_generator
from libs.utils.date_utils import daterange
from libs.utils.form_utils import CommaSeparatedListCharField, CommaSeparatedListChoiceField

forest_pages = Blueprint('forest_pages', __name__)


@forest_pages.context_processor
def inject_html_params():
    # these variables will be accessible to every template rendering attached to the blueprint
    return {
        "allowed_studies": get_researcher_allowed_studies(),
        "is_admin": researcher_is_an_admin(),
    }


@forest_pages.route('/studies/<string:study_id>/forest/progress', methods=['GET'])
@authenticate_researcher_study_access
@forest_enabled
def analysis_progress(study_id=None):
    study = Study.objects.get(pk=study_id)
    participants = Participant.objects.filter(study=study_id)

    # generate chart of study analysis progress logs
    trackers = ForestTask.objects.filter(participant__in=participants).order_by("created_on")

    start_date = study.get_inferred_start_date()
    end_date = study.get_inferred_latest_date()

    # this code simultaneously builds up the chart of most recent forest results for date ranges
    # by participant and tree, and tracks the metadata
    params = dict()
    results = defaultdict(lambda: "--")
    for tracker in trackers:
        for date in daterange(tracker.data_date_start, tracker.data_date_end, inclusive=True):
            results[(tracker.participant_id, tracker.forest_tree, date)] = tracker.status
            if tracker.status == tracker.Status.success:
                params[(tracker.participant_id, tracker.forest_tree, date)] = tracker.forest_param_id
            else:
                params[(tracker.participant_id, tracker.forest_tree, date)] = None

    # generate the date range for charting
    dates = list(daterange(start_date, end_date, inclusive=True))

    chart_columns = ["participant", "tree"] + dates
    chart = []

    for participant in participants:
        for tree in ForestTree.values():
            row = [participant.patient_id, tree] + [results[(participant.id, tree, date)] for date in dates]
            chart.append(row)

    params_conflict = False
    # ensure that within each tree, only a single set of param values are used (only the most recent runs
    # are considered, and unsuccessful runs are assumed to invalidate old runs, clearing params)
    for tree in set([k[1] for k in params.keys()]):
        if len(set([m for k, m in params.items() if m is not None and k[1] == tree])) > 1:
            params_conflict = True
            break

    return render_template(
        'forest/analysis_progress.html',
        study=study,
        chart_columns=chart_columns,
        status_choices=ForestTask.Status,
        params_conflict=params_conflict,
        start_date=start_date,
        end_date=end_date,
        chart=chart  # this uses the jinja safe filter and should never involve user input
    )


class CreateTasksForm(forms.Form):
    date_start = forms.DateField()
    date_end = forms.DateField()
    participant_patient_ids = CommaSeparatedListCharField()
    trees = CommaSeparatedListChoiceField(choices=ForestTree.choices())
    
    def __init__(self, *args, **kwargs):
        self.study = kwargs.pop("study")
        if "data" in kwargs:
            if isinstance(kwargs["data"], MultiDict):
                # Convert Flask/Werkzeug MultiDict format into comma-separated values. This is
                # to allow Flask's handling of multi inputs to work with Django's form data
                # structures.
                kwargs["data"] = {
                    key: ",".join(value)
                    for key, value in kwargs["data"].to_dict(flat=False).items()
                }
        super().__init__(*args, **kwargs)
    
    def clean(self):
        cleaned_data = super().clean()
        if cleaned_data["date_end"] < cleaned_data["date_start"]:
            error_message = "Start date must be before or the same as end date."
            self.add_error("date_start", error_message)
            self.add_error("date_end", error_message)
    
    def clean_participant_patient_ids(self):
        """
        Filter participants to those who are registered in this study and specified in this field
        (instead of raising a ValidationError if an invalid or non-study patient id is specified).
        """
        patient_ids = self.cleaned_data["participant_patient_ids"]
        participants = (
            Participant
                .objects
                .filter(patient_id__in=patient_ids, study=self.study)
                .values("id", "patient_id")
        )
        self.cleaned_data["participant_ids"] = [participant["id"] for participant in participants]
        
        return [participant["patient_id"] for participant in participants]

    def save(self):
        forest_tasks = []
        for participant_id in self.cleaned_data["participant_ids"]:
            for tree in self.cleaned_data["trees"]:
                forest_tasks.append(
                    ForestTask(
                        participant_id=participant_id,
                        forest_tree=tree,
                        data_date_start=self.cleaned_data["date_start"],
                        data_date_end=self.cleaned_data["date_end"],
                        status=ForestTask.Status.queued,
                        forest_param=self.study.forest_param,
                    )
                )
        ForestTask.objects.bulk_create(forest_tasks)


@forest_pages.route('/studies/<string:study_id>/forest/tasks/create', methods=['GET', 'POST'])
@authenticate_admin
@forest_enabled
def create_tasks(study_id=None):
    # Only a SITE admin can queue forest tasks
    if not get_session_researcher().site_admin:
        return abort(403)
    try:
        study = Study.objects.get(pk=study_id)
    except Study.DoesNotExist:
        return abort(404)
        
    if request.method == "GET":
        return _render_create_tasks(study)
    
    form = CreateTasksForm(data=request.values, study=study)

    if not form.is_valid():
        error_messages = [
            f'"{field}": {message}'
            for field, messages in form.errors.items()
            for message in messages
        ]
        error_messages_string = "\n".join(error_messages)
        flash(f"Errors:\n\n{error_messages_string}", "warning")
        return _render_create_tasks(study)
    
    form.save()
    flash("Forest tasks successfully queued!", "success")
    return redirect(url_for("forest_pages.task_log", study_id=study_id))


def _render_create_tasks(study):
    start_date = study.get_inferred_start_date()
    end_date = study.get_inferred_latest_date()
    return render_template(
        "forest/create_tasks.html",
        study=study,
        participants=list(
            study.participants.order_by("patient_id").values_list("patient_id", flat=True)
        ),
        trees=ForestTree.choices(),
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d')
    )

@forest_pages.route('/studies/<string:study_id>/forest/tasks', methods=["GET"])
@authenticate_researcher_study_access
@forest_enabled
def task_log(study_id=None):
    study = Study.objects.get(pk=study_id)
    forest_tasks = (
        ForestTask
            .objects
            .filter(participant__study_id=study_id)
            .order_by("-created_on")
    )
    return render_template(
        "forest/task_log.html",
        study=study,
        is_site_admin=get_session_researcher().site_admin,
        status_choices=ForestTask.Status,
        forest_log=ForestTaskSerializer(forest_tasks, many=True).data,
    )



class CSVBuffer:
    line = ""
    
    def read(self):
        return self.line
    
    def write(self, line):
        self.line = line


def stream_forest_task_log_csv(forest_tasks):
    buffer = CSVBuffer()
    writer = csv.DictWriter(buffer, fieldnames=ForestTaskCsvSerializer.Meta.fields)
    writer.writeheader()
    yield buffer.read()
    from app import app
    with app.test_request_context():
        for forest_task in forest_tasks:
            writer.writerow(ForestTaskCsvSerializer(forest_task).data)
            yield buffer.read()


@forest_pages.route('/forest/tasks/download', methods=["GET"])
@authenticate_admin
def download_task_log():
    filename = f"forest_task_log_{timezone.now().isoformat()}.csv"
    forest_tasks = ForestTask.objects.order_by("created_on")
    return Response(
        stream_forest_task_log_csv(forest_tasks),
        headers={"Content-Disposition": f"attachment; filename=\"{filename}\""},
        mimetype="text/csv",
    )


@forest_pages.route("/studies/<string:study_id>/forest/tasks/<string:forest_task_external_id>/cancel", methods=["POST"])
@authenticate_admin
@forest_enabled
def cancel_task(study_id, forest_task_external_id):
    number_updated = (
        ForestTask
            .objects
            .filter(external_id=forest_task_external_id, status=ForestTask.Status.queued)
            .update(
                status=ForestTask.Status.cancelled,
                stacktrace=f"Canceled by {get_session_researcher().username} on {datetime.date.today()}",
            )
    )
    if number_updated > 0:
        flash("Forest task successfully cancelled.", "success")
    else:
        flash("Sorry, we were unable to find or cancel this Forest task.", "warning")

    return redirect(url_for("forest_pages.task_log", study_id=study_id))


@forest_pages.route("/studies/<string:study_id>/forest/tasks/<string:forest_task_external_id>/download", methods=["GET"])
@authenticate_admin
@forest_enabled
def download_task_data(study_id, forest_task_external_id):
    try:
        tracker = ForestTask.objects.get(
            external_id=forest_task_external_id,
            participant__study_id=study_id,
        )
    except ForestTask.DoesNotExist:
        return abort(404)

    chunks = ChunkRegistry.objects.filter(participant=tracker.participant).values(*chunk_fields)
    return Response(
        zip_generator(chunks),
        headers={"Content-Disposition": f"attachment; filename=\"{tracker.get_slug()}.zip\""},
        mimetype="zip",
    )
