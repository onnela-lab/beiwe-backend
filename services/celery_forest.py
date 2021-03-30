import json
import os
import shutil
import traceback
from datetime import datetime, timedelta

from cronutils.error_handler import NullErrorHandler
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone
from forest.jasmine.traj2stats import gps_stats_main as jasmine_main
from forest.willow.log_stats import log_stats_main as willow_main
from pkg_resources import get_distribution

from config.constants import FOREST_QUEUE
from database.data_access_models import ChunkRegistry
from database.tableau_api_models import ForestTracker
from libs.celery_control import forest_celery_app, safe_apply_async
from libs.forest_integration.forest_data_interpretation import construct_summary_statistics


# run via cron every five minutes
def create_forest_celery_tasks():
    pending_trackers = ForestTracker.objects.filter(status=ForestTracker.Status.QUEUED)

    # with make_error_sentry(sentry_type=SentryTypes.data_processing):  # add a new type?
    with NullErrorHandler():  # for debugging, does not suppress errors
        for tracker in pending_trackers:
            print(f"Queueing up celery task for {tracker.participant} on tree {tracker.forest_tree} from {tracker.data_date_start} to {tracker.data_date_end}")
            enqueue_forest_task(args=[tracker.id])


#run via celery as long as tasks exist
@forest_celery_app.task(queue=FOREST_QUEUE)
def celery_run_forest(forest_tracker_id):
    with transaction.atomic():
        tracker = ForestTracker.objects.filter(id=forest_tracker_id).first()

        participant = tracker.participant
        forest_tree = tracker.forest_tree
        
        # If there is already a running task for this participant and tree, requeue and exit
        trackers = ForestTracker.objects.select_for_update().filter(participant=participant, forest_tree=forest_tree)
        if trackers.filter(status=ForestTracker.Status.RUNNING).exists():
            enqueue_forest_task(args=[tracker.id])
            return
        
        # Get the chronologically earliest tracker that's queued
        tracker = (
            trackers
                .filter(status=ForestTracker.Status.QUEUED)
                .order_by("-data_date_start")
                .first()
        )
        if tracker is None:
            return
        tracker.status = ForestTracker.Status.RUNNING
        tracker.forest_version = get_distribution("forest").version
        tracker.process_start_time = timezone.now()
        tracker.save(update_fields=["status", "forest_version", "process_start_time"])

    data = ChunkRegistry.objects.filter(participant=participant)
    tracker.total_file_size = data.aggregate(Sum('file_size')).get('file_size__sum')
    print(f"collecting data. running task from celery on tracker {tracker.id}")
    try:
        create_local_data_files(tracker, data)
        tracker.process_download_end_time = timezone.now()
        params = {
            'study_folder': tracker.get_data_input_folder(),
            'output_folder': tracker.get_data_output_folder(),
            'time_start': tracker.data_date_start,
            'time_end': tracker.data_date_end,
        }
        forest_output = ''
        if tracker.forest_tree == "willow":
            # the following merges the computed parameters with the ones from the metadata object,
            # preferring the newly computed parameters
            params = {**json.loads(tracker.metadata.willow_json_string), **params}
            forest_output = willow_main(**params)
        if tracker.forest_tree == "jasmine":
            params = {**json.loads(tracker.metadata.jasmine_json_string), **params}
            forest_output = jasmine_main(**params)
        construct_summary_statistics(tracker, forest_output)
    except Exception:
        tracker.status = tracker.Status.ERROR
        tracker.stacktrace = traceback.format_exc()
    else:
        tracker.status = tracker.Status.SUCCESS
    tracker.process_end_time = timezone.now()
    tracker.save()
    # Todo (Alvin): put this back in after testing
    # clean_local_data_files(tracker.get_data_base_folder())


def create_local_data_files(tracker, data):
    for data_file in data:
        file_name = os.path.join(
            tracker.get_data_input_folder(),
            "".join(filter(lambda c: c != "/", data_file.chunk_hash)) + ".txt",
        )
        contents = data_file.s3_retrieve()
        with open(file_name, "x") as f:
            f.write(contents.decode("utf-8"))


def clean_local_data_files(io_folder):
    shutil.rmtree(io_folder)  # this is equivalent to rm -r, double check any changes made here


def enqueue_forest_task(**kwargs):
    updated_kwargs = {
        "expires": (datetime.utcnow() + timedelta(minutes=5)).replace(second=30, microsecond=0),
        "max_retries": 0,
        "retry": False,
        "task_publish_retry": False,
        "task_track_started": True,
        **kwargs,
    }
    safe_apply_async(celery_run_forest, **updated_kwargs)
