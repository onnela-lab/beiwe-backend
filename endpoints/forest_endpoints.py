import csv
import pickle
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, List

import orjson
from django.contrib import messages
from django.core.exceptions import ValidationError
from django.db.models import F, QuerySet
from django.http import StreamingHttpResponse
from django.http.response import FileResponse, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from authentication.admin_authentication import (assert_site_admin, authenticate_admin,
    authenticate_researcher_study_access, forest_enabled)
from constants.celery_constants import ForestTaskStatus
from constants.common_constants import DEV_TIME_FORMAT, EARLIEST_POSSIBLE_DATA_DATE, RUNNING_TESTS
from constants.forest_constants import (FOREST_NO_TASK, FOREST_TASK_CANCELLED,
    FOREST_TASKVIEW_PICKLING_EMPTY, FOREST_TASKVIEW_PICKLING_ERROR,
    FOREST_TREE_REQUIRED_DATA_STREAMS, FOREST_TREE_TO_SERIALIZABLE_FIELD_NAMES, ForestTree,
    NICE_SERIALIZABLE_FIELD_NAMES, SERIALIZABLE_FIELD_NAMES)
from constants.raw_data_constants import CHUNK_FIELDS
from database.data_access_models import ChunkRegistry
from database.forest_models import ForestTask, SummaryStatisticDaily
from database.study_models import Study
from database.system_models import ForestVersion
from database.user_models_participant import Participant
from libs.django_forms.forms import CreateTasksForm
from libs.efficient_paginator import EfficientQueryPaginator
from libs.endpoint_helpers.dashboard_helpers import get_first_and_last_days_of_data
from libs.endpoint_helpers.summary_statistic_helpers import SummaryStatisticsPaginator
from libs.internal_types import ParticipantQuerySet, ResearcherRequest
from libs.s3 import NoSuchKeyException
from libs.streaming_io import CSVBuffer
from libs.streaming_zip import ZipGenerator
from libs.utils.date_utils import daterange
from libs.utils.forest_utils import download_output_file
from libs.utils.http_utils import easy_url


TASK_SERIALIZER_FIELDS = [
    # raw
    "data_date_end",
    "data_date_start",
    "forest_output_exists",
    "id",
    "stacktrace",
    "status",
    "total_file_size",
    # to be popped
    "external_id",  # -> uuid in the urls
    "participant__patient_id",  # -> patient_id
    "pickled_parameters",
    "forest_tree",  # -> forest_tree_display as .title()
    "forest_commit",
    "output_zip_s3_path",  # need to identify that it is present at all
    # datetimes
    "process_end_time",  # -> dev time format
    "process_start_time",  # -> dev time format
    "process_download_end_time",  # -> dev time format
    "created_on",  # -> dev time format
]

@require_GET
@authenticate_researcher_study_access
@forest_enabled
def forest_tasks_progress(request: ResearcherRequest, study_id=None):
    # generates a chart of study analysis progress logs
    study = Study.objects.get(pk=study_id)
    participants: ParticipantQuerySet = Participant.objects.filter(study=study_id)
    
    # later tasks will overwrite earlier tasks - this is intentional.
    # number of forest tasks shouldn't be the bottleneck here.
    tasks = ForestTask.objects.filter(participant__in=participants).order_by("created_on")
    
    results = defaultdict(lambda: "-")
    chart_elements_lookup = {False: "N", None: "?"}
    # this loop builds the chart of whether there are forest results for date ranges
    # per-participant- -and-tree. The tasks query is ordered by creation date, so later tasks will
    # overwrite earlier tasks. a "-" means no task has been run, "N" means a task ran but there was
    # no output, "?" means there the code ran successfully but there was an error reading in the
    # data so we MIGHT have data, and "Y" means there was definitely data.
    for task in tasks:
        for a_date in daterange(task.data_date_start, task.data_date_end, inclusive=True):
            key = (task.participant_id, task.forest_tree, a_date)
            in_table = results[key]  # will populates with a "-" on first access
            output_exists = task.forest_output_exists
            if in_table == "Y" or output_exists:  # always force "Y"
                results[key] = "Y"
            elif in_table != "?":
                # We have some nice constraints here:
                # 1. output_exists is False or None, so chart_elements_lookup[false or None]
                #    can only return "N" or "?".
                # 2. The chart's current field is "-", "N", or "?"
                # 3. If in_table is a ? we can just skip it because we cannot upgrade from ? to Y here.
                #  So, we just skip if we are already at ? in the chart element, and otherwise we do the lookup.
                results[key] = chart_elements_lookup[output_exists]
    
    start_date, end_date = get_first_and_last_days_of_data(study)
    start_date = start_date or study.created_on.date()
    end_date = end_date or timezone.now().date()
    
    # generate the date range for the chart, we need it many times.
    dates = list(daterange(start_date, end_date, inclusive=True))
    chart = []
    for participant in participants:
        for tree_name in ForestTree.values():
            # we need to make a list of lists with the participant and tree name
            row = [participant.patient_id, tree_name] + \
                [results[(participant.id, tree_name, date)] for date in dates]
            chart.append(row)
    
    chart_json = orjson.dumps(chart).decode()  # may be huge, but orjson is very fast.
    return render(
        request,
        'forest/forest_tasks_progress.html',  # has been renamed internally because this is imprecise.
        context=dict(
            study=study,
            chart_columns=["participant", "tree"] + dates,
            status_choices=ForestTaskStatus,
            start_date=start_date,
            end_date=end_date,
            chart=chart_json  # this uses the jinja safe filter and should never involve user input
        )
    )


@require_http_methods(['GET', 'POST'])
@authenticate_admin
@forest_enabled
def create_tasks(request: ResearcherRequest, study_id=None):
    assert_site_admin(request)  # Only a SITE admin can queue forest tasks
    
    try:
        study = Study.objects.get(pk=study_id)
    except Study.DoesNotExist:
        return HttpResponse(content="", status=404)
    
    # FIXME: remove this double endpoint pattern, it is bad.
    if request.method == "GET":
        return render_create_tasks(request, study)
    form = CreateTasksForm(data=request.POST, study=study)
    
    if not form.is_valid():
        error_messages = [
            f'"{field}": {message}'
            for field, messages in form.errors.items()
            for message in messages
        ]
        error_messages_string = "\n".join(error_messages)
        messages.warning(request, f"Errors:\n\n{error_messages_string}")
        return render_create_tasks(request, study)
    
    form.save()
    messages.success(request, "Forest tasks successfully queued!")
    return redirect(easy_url("forest_endpoints.task_log", study_id=study_id))


@require_http_methods(['GET', 'POST'])
@authenticate_admin
@forest_enabled
def copy_forest_task(request: ResearcherRequest, study_id=None):
    # Only a SITE admin can queue forest tasks
    if not request.session_researcher.site_admin:
        return HttpResponse(content="", status=403)
    try:
        study = Study.objects.get(pk=study_id)
    except Study.DoesNotExist:
        return HttpResponse(content="", status=404)
    
    task_id = request.POST.get("external_id", None)
    if not task_id:
        messages.warning(request, FOREST_NO_TASK)
        return redirect(easy_url("forest_endpoints.task_log", study_id=study_id))
    
    try:
        task_to_copy = ForestTask.objects.get(external_id=task_id)
    except (ForestTask.DoesNotExist, ValidationError):
        messages.warning(request, FOREST_NO_TASK)
        return redirect(easy_url("forest_endpoints.task_log", study_id=study_id))
    
    new_task = ForestTask(
        participant=task_to_copy.participant,
        forest_tree=task_to_copy.forest_tree,
        data_date_start=task_to_copy.data_date_start,
        data_date_end=task_to_copy.data_date_end,
        status=ForestTaskStatus.queued,
    )
    new_task.save()
    messages.success(
        request, f"Made a copy of {task_to_copy.external_id} with id {new_task.external_id}."
    )
    return redirect(easy_url("forest_endpoints.task_log", study_id=study_id))


@require_GET
@authenticate_researcher_study_access
@forest_enabled
def task_log(request: ResearcherRequest, study_id=None):
    query = ForestTask.objects.filter(participant__study_id=study_id)\
        .order_by("-created_on").values(*TASK_SERIALIZER_FIELDS)
    tasks = []
    
    for task_dict in query:
        extern_id = task_dict["external_id"]
        
        # the commit is populated when the task runs, not when it is queued.
        task_dict["forest_commit"] = task_dict["forest_commit"] if task_dict["forest_commit"] else \
            "(exact commit missing)"
        
        # renames (could be optimized in the query, but speedup is negligible)
        task_dict["patient_id"] = task_dict.pop("participant__patient_id")
        
        # rename and transform
        task_dict["has_output_data"] = task_dict["forest_output_exists"]
        task_dict["download_participant_tree_data_url"] = easy_url(
            "forest_endpoints.download_participant_tree_data", study_id=study_id, forest_task_external_id=extern_id,
        )
        task_dict["forest_tree_display"] = task_dict.pop("forest_tree").title()
        task_dict["created_on_display"] = task_dict.pop("created_on").strftime(DEV_TIME_FORMAT)
        task_dict["forest_output_exists_display"] = yes_no_unknown(task_dict["forest_output_exists"])
        
        # dates/times that require safety (yes it could be less obnoxious)
        dict_datetime_to_display(task_dict, "process_end_time", None)
        dict_datetime_to_display(task_dict, "process_start_time", None)
        dict_datetime_to_display(task_dict, "process_download_end_time", None)
        task_dict["data_date_end"] = task_dict["data_date_end"].isoformat() if task_dict["data_date_end"] else None
        task_dict["data_date_start"] = task_dict["data_date_start"].isoformat() if task_dict["data_date_start"] else None
        
        # urls
        task_dict["cancel_url"] = easy_url(
            "forest_endpoints.cancel_task", study_id=study_id, forest_task_external_id=extern_id,
        )
        task_dict["copy_url"] = easy_url("forest_endpoints.copy_forest_task", study_id=study_id)
        task_dict["download_url"] = easy_url(
            "forest_endpoints.download_task_data", study_id=study_id, forest_task_external_id=extern_id,
        )
        
        # raw output data data is only available if the task has completed successfully, and not
        # on older tasks that were run before we started saving the output data.
        if task_dict.pop("output_zip_s3_path"):
            task_dict["has_output_downloadable_data"] = True
            task_dict["download_output_url"] = easy_url(
                "forest_endpoints.download_output_data", study_id=study_id, forest_task_external_id=extern_id,
            )
        
        # the pickled parameters have some error cases.
        if task_dict["pickled_parameters"]:
            try:
                task_dict["params_dict"] = repr(pickle.loads(task_dict.pop("pickled_parameters")))
            except Exception:
                task_dict["params_dict"] = FOREST_TASKVIEW_PICKLING_ERROR
        else:
            task_dict["params_dict"] = FOREST_TASKVIEW_PICKLING_EMPTY
        tasks.append(task_dict)
    forest_info = ForestVersion.singleton()
    return render(
        request,
        "forest/task_log.html",
        context=dict(
            study=Study.objects.get(pk=study_id),
            status_choices=ForestTaskStatus,
            forest_log=orjson.dumps(tasks).decode(),  # orjson is very fast and handles the remaining date objects
            forest_commit=forest_info.git_commit or "commit not found",
            forest_version=forest_info.package_version or "version not found",
        )
    )


@require_POST
@authenticate_admin
@forest_enabled
def cancel_task(request: ResearcherRequest, study_id: int, forest_task_external_id: str):
    if not request.session_researcher.site_admin:
        return HttpResponse(content="", status=403)
    
    try:
        number_updated = ForestTask.objects.filter(
            external_id=forest_task_external_id, status=ForestTaskStatus.queued
        ).update(
            status=ForestTaskStatus.cancelled,
            stacktrace=f"Canceled by {request.session_researcher.username} on {date.today()}",
        )
    except ValidationError:
        # malformed uuids throw a validation error
        number_updated = 0
    
    if number_updated > 0:
        messages.success(request, FOREST_TASK_CANCELLED)
    else:
        messages.warning(request, FOREST_NO_TASK)
    
    return redirect(easy_url("forest_endpoints.task_log", study_id=study_id))


@require_GET
@authenticate_admin
@forest_enabled
def download_task_data(request: ResearcherRequest, study_id: int, forest_task_external_id: str):
    try:
        forest_task: ForestTask = ForestTask.objects.get(
            external_id=forest_task_external_id, participant__study_id=study_id
        )
    except ForestTask.DoesNotExist:
        return HttpResponse(content="", status=404)
    
    # this time manipulation is copied right out of the celery forest task runner.
    start_time_midnight = datetime.combine(
        forest_task.data_date_start, datetime.min.time(), forest_task.participant.study.timezone
    )
    endtime_11_59pm = datetime.combine(
        forest_task.data_date_end, datetime.max.time(), forest_task.participant.study.timezone
    )
    
    chunks: str = ChunkRegistry.objects.filter(
        participant=forest_task.participant,
        time_bin__gte=start_time_midnight,
        time_bin__lt=endtime_11_59pm,  # inclusive
        data_type__in=FOREST_TREE_REQUIRED_DATA_STREAMS[forest_task.forest_tree]
    ).values(*CHUNK_FIELDS)
    
    filename = "_".join([
            forest_task.participant.patient_id,
            forest_task.forest_tree,
            str(forest_task.data_date_start),
            str(forest_task.data_date_end),
            "data",
        ]) + ".zip"
    
    f = FileResponse(
        ZipGenerator(chunks, False),
        content_type="zip",
        as_attachment=True,
        filename=filename,
    )
    f.set_headers(None)  # this is just a thing you have to do, its a django bug.
    return f


@require_GET
@authenticate_admin
@forest_enabled
def download_output_data(request: ResearcherRequest, study_id: int, forest_task_external_id: str):
    try:
        forest_task: ForestTask = ForestTask.objects.get(
            external_id=forest_task_external_id, participant__study_id=study_id
        )
    except (ForestTask.DoesNotExist, ValidationError):
        return HttpResponse(content="", status=404)
    
    filename = "_".join([
            forest_task.participant.patient_id,
            forest_task.forest_tree,
            str(forest_task.data_date_start),
            str(forest_task.data_date_end),
            "output",
            forest_task.created_on.strftime(DEV_TIME_FORMAT),
        ]) + ".zip"
    
    try:
        file_content = download_output_file(forest_task)
    except NoSuchKeyException:
        # limit the error scope we are catching here, we want those errors reported.
        return HttpResponse(content="Unable to find report file. ¯\\_(ツ)_/¯", status=404)
    
    # for some reason FileResponse doesn't work when handed a bytes object, so we are forcing the
    # headers for attachment and filename in a custom HttpResponse. Baffling. I guess FileResponse
    # is really only for file-like objects.
    return HttpResponse(
        file_content,
        content_type="zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@require_GET
@authenticate_admin
@forest_enabled
def download_participant_tree_data(request: ResearcherRequest, study_id: int, forest_task_external_id: str):
    """ Downloads a csv representation of a participant's data for a specific forest tree. """
    # study id is validated in the authenticate_admin decorator, but its not tied to the forest task
    try:
        forest_task: ForestTask = ForestTask.objects.get(external_id=forest_task_external_id)
        participant = forest_task.participant
    except (ForestTask.DoesNotExist, ValidationError):
        return HttpResponse(content="", status=404)
    
    if participant.study.id != int(study_id):
        return HttpResponse(content="correct 404 case" if RUNNING_TESTS else "", status=404)
    
    if forest_task.forest_tree not in FOREST_TREE_TO_SERIALIZABLE_FIELD_NAMES:
        return HttpResponse(content="No such forest tree found.", status=404)
    
    # get the database fields and construct values for the csv header
    fields_names: List[str] = ["date"] + FOREST_TREE_TO_SERIALIZABLE_FIELD_NAMES[forest_task.forest_tree]
    nice_names = [
        # e.g. jasmine_distance_from_home -> Distance From Home
        name.replace(f"{forest_task.forest_tree}_", "").replace("_", " ").title()
            for name in fields_names
    ]
    
    # protect our users from themselves, handle case of no data with a conformant header plus newline
    if not participant.summarystatisticdaily_set.exists():
        return HttpResponse(content=",".join(nice_names) + "\r\n", status=200)
    
    paginator = EfficientQueryPaginator(
        participant.summarystatisticdaily_set.order_by("date"), 10000, values_list=fields_names
    )
    
    contextually_accurate_date = participant.study.now().date()  # studies have time zones, use that
    
    f = FileResponse(
        paginator.stream_csv(nice_names),
        content_type="text/csv",
        as_attachment=True,
        filename=f"{participant.patient_id}_{forest_task.forest_tree}_data_{contextually_accurate_date}.csv",
    )
    f.set_headers(None)  # this is just a thing you have to do, its a django bug.
    return f


@require_GET
@authenticate_admin
@forest_enabled
def download_summary_statistics_csv(request: ResearcherRequest, study_id):
    study = Study.objects.get(pk=study_id)  # study id already validated in authenticate_admin()
    # we need to rename two fields like we do over in the tableau api
    query = SummaryStatisticDaily.objects.filter(participant__study_id=study.id)\
        .order_by("participant__patient_id", "date")\
        .annotate(
            study_id=F("participant__study__object_id"),
            patient_id=F("participant__patient_id"),
        )
    # same a s SERIALIZABLE_FIELD_NAMES but replace patient_id with participant_id
    query_field_names = SERIALIZABLE_FIELD_NAMES.copy()
    query_field_names[query_field_names.index("participant_id")] = "patient_id"
    
    paginator = SummaryStatisticsPaginator(query, 10000, values_list=query_field_names)
    return StreamingHttpResponse(paginator.stream_csv(NICE_SERIALIZABLE_FIELD_NAMES), content_type="text/csv")


def render_create_tasks(request: ResearcherRequest, study: Study):
    # this is the fastest way to get earliest and latest dates, even for large numbers of matches.
    # SummaryStatisticDaily is orders of magnitude smaller than ChunkRegistry.
    dates = list(
        SummaryStatisticDaily.objects
        .exclude(date__lte=max(EARLIEST_POSSIBLE_DATA_DATE, study.created_on.date()))
        .filter(participant__in=study.participants.all())
        .order_by("date")
        .values_list("date", flat=True)
    )
    start_date = dates[0] if dates else study.created_on.date()
    end_date = dates[-1] if dates else timezone.now().date()
    forest_info = ForestVersion.singleton()
    
    # start_date = dates[0] if dates and dates[0] >= EARLIEST_POSSIBLE_DATA_DATE else study.created_on.date()
    # end_date = dates[-1] if dates and dates[-1] <= timezone.now().date() else timezone.now().date()
    return render(
        request,
        "forest/create_tasks.html",
        context=dict(
            study=study,
            participants=list(
                study.participants.order_by("patient_id").values_list("patient_id", flat=True)
            ),
            trees=ForestTree.choices(),
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            forest_version=forest_info.package_version.title(),
            forest_commit=forest_info.git_commit,
        )
    )


@require_GET
@authenticate_admin
def download_task_log(request: ResearcherRequest, study_id=str):
    if not request.session_researcher.site_admin:
        return HttpResponse(content="", status=403)
    
    # study id is already validated by the url pattern?
    forest_tasks = ForestTask.objects.filter(participant__study_id=study_id)
    
    return FileResponse(
        stream_forest_task_log_csv(forest_tasks),
        content_type="text/csv",
        filename=f"forest_task_log_{timezone.now().isoformat()}.csv",
        as_attachment=True,
    )


def stream_forest_task_log_csv(forest_tasks: QuerySet[ForestTask]):
    # titles of rows as values, query filter values as keys
    field_map = {
        "created_on": "Created On",
        "data_date_end": "Data Date End",
        "data_date_start": "Data Date Start",
        "external_id": "Id",
        "forest_tree": "Forest Tree",
        "forest_output_exists": "Forest Output Exists",
        "participant__patient_id": "Patient Id",
        "process_start_time": "Process Start Time",
        "process_download_end_time": "Process Download End Time",
        "process_end_time": "Process End Time",
        "status": "Status",
        "total_file_size": "Total File Size",
    }
    
    # setup
    buffer = CSVBuffer()
    # the csv writer isn't well handled in the vscode ide. its has a writerow and writerows method,
    # writes to a file-like object, CSVBuffer might be overkill, strio or bytesio might be work
    writer = csv.writer(buffer, dialect="excel")
    writer.writerow(field_map.values())
    yield buffer.read()  # write the header
    
    # yield rows
    for forest_data in forest_tasks.values(*field_map.keys()):
        dict_datetime_to_display(forest_data, "created_on", "")
        dict_datetime_to_display(forest_data, "process_start_time", "")
        dict_datetime_to_display(forest_data, "process_download_end_time", "")
        dict_datetime_to_display(forest_data, "process_end_time", "")
        forest_data["forest_tree"] = forest_data["forest_tree"].title()
        forest_data["forest_output_exists"] = yes_no_unknown(forest_data["forest_output_exists"])
        writer.writerow(forest_data.values())
        yield buffer.read()


def dict_datetime_to_display(some_dict: Dict[str, datetime], key: str, default: str = None):
    # this pattern is repeated numerous times.
    dt = some_dict[key]
    if dt is None:
        some_dict[key] = default
    else:
        some_dict[key] = dt.strftime(DEV_TIME_FORMAT)


def yes_no_unknown(a_bool: bool):
    if a_bool is True:
        return "Yes"
    elif a_bool is False:
        return "No"
    else:
        return "Unknown"
