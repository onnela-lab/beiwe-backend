import json
import os
import traceback
from datetime import datetime, timedelta

from cronutils.error_handler import NullErrorHandler
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone
from forest.jasmine.traj2stats import gps_stats_main
from forest.willow.log_stats import log_stats_main
from pkg_resources import get_distribution

from api.data_access_api import chunk_fields
from config.constants import FOREST_QUEUE
from database.data_access_models import ChunkRegistry
from database.tableau_api_models import ForestTracker
from libs.celery_control import forest_celery_app, safe_apply_async
from libs.forest_integration.constants import ForestTree
from libs.forest_integration.forest_data_interpretation import construct_summary_statistics


# run via cron every five minutes
from libs.s3 import s3_retrieve
from libs.streaming_zip import determine_file_name


TREE_TO_FOREST_FUNCTION = {
    ForestTree.jasmine: gps_stats_main,
    ForestTree.willow: log_stats_main,
}


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

    chunks = ChunkRegistry.objects.filter(participant=participant)
    tracker.total_file_size = chunks.aggregate(Sum('file_size')).get('file_size__sum')
    tracker.save(update_fields=["total_file_size"])
    
    try:
        create_local_data_files(tracker, chunks)
        tracker.process_download_end_time = timezone.now()
        tracker.save(update_field=["process_download_end_time"])

        TREE_TO_FOREST_FUNCTION[tracker.forest_tree](**tracker.forest_params())
        construct_summary_statistics(tracker)
    
    except Exception:
        tracker.status = tracker.Status.ERROR
        tracker.stacktrace = traceback.format_exc()
    else:
        tracker.status = tracker.Status.SUCCESS
    tracker.process_end_time = timezone.now()
    tracker.save()
    
    tracker.clean_up_files()


def create_local_data_files(tracker, chunks):
    for chunk in chunks.values("study__object_id", *chunk_fields):
        contents = s3_retrieve(chunk["chunk_path"], chunk["study__object_id"], raw_path=True)
        file_name = os.path.join(
            tracker.data_input_path,
            determine_file_name(chunk),
        )
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, "x") as f:
            f.write(contents.decode("utf-8"))


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
