import json
import os
import traceback
from datetime import datetime, timedelta

from cronutils.error_handler import NullErrorHandler
from django.core.serializers.json import DjangoJSONEncoder
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone
from forest.jasmine.traj2stats import gps_stats_main
from forest.willow.log_stats import log_stats_main
from pkg_resources import get_distribution

from api.data_access_api import chunk_fields
from config.constants import FOREST_QUEUE
from database.data_access_models import ChunkRegistry
from database.tableau_api_models import ForestTask
from libs.celery_control import forest_celery_app, safe_apply_async
from libs.forest_integration.constants import ForestTree


# run via cron every five minutes
from libs.s3 import s3_retrieve
from libs.streaming_zip import determine_file_name


TREE_TO_FOREST_FUNCTION = {
    ForestTree.jasmine: gps_stats_main,
    ForestTree.willow: log_stats_main,
}


def create_forest_celery_tasks():
    pending_trackers = ForestTask.objects.filter(status=ForestTask.Status.queued)

    # with make_error_sentry(sentry_type=SentryTypes.data_processing):  # add a new type?
    with NullErrorHandler():  # for debugging, does not suppress errors
        for tracker in pending_trackers:
            print(f"Queueing up celery task for {tracker.participant} on tree {tracker.forest_tree} from {tracker.data_date_start} to {tracker.data_date_end}")
            enqueue_forest_task(args=[tracker.id])


#run via celery as long as tasks exist
@forest_celery_app.task(queue=FOREST_QUEUE)
def celery_run_forest(forest_task_id):
    with transaction.atomic():
        tracker = ForestTask.objects.filter(id=forest_task_id).first()

        participant = tracker.participant
        forest_tree = tracker.forest_tree
        
        # Check if there already is a running task for this participant and tree, handling
        # concurrency and requeuing of the ask if necessary
        trackers = (
            ForestTask
                .objects
                .select_for_update()
                .filter(participant=participant, forest_tree=forest_tree)
        )
        if trackers.filter(status=ForestTask.Status.running).exists():
            enqueue_forest_task(args=[tracker.id])
            return
        
        # Get the chronologically earliest tracker that's queued
        tracker = (
            trackers
                .filter(status=ForestTask.Status.queued)
                .order_by("-data_date_start")
                .first()
        )
        if tracker is None:
            return
        
        # Set metadata on the tracker
        tracker.status = ForestTask.Status.running
        tracker.forest_version = get_distribution("forest").version
        tracker.process_start_time = timezone.now()
        tracker.save(update_fields=["status", "forest_version", "process_start_time"])

    try:
        # Save file size data
        # The largest UTC offsets are -12 and +14
        min_datetime = datetime.combine(tracker.data_date_start, datetime.min.time()) - timedelta(hours=12)
        max_datetime = datetime.combine(tracker.data_date_end, datetime.max.time()) + timedelta(hours=14)
        chunks = (
            ChunkRegistry
                .objects
                .filter(participant=participant)
                .filter(time_bin__gte=min_datetime)
                .filter(time_bin__lte=max_datetime)
        )
        tracker.total_file_size = chunks.aggregate(Sum('file_size')).get('file_size__sum')
        tracker.save(update_fields=["total_file_size"])
    
        # Download data
        create_local_data_files(tracker, chunks)
        tracker.process_download_end_time = timezone.now()
        tracker.save(update_fields=["process_download_end_time"])

        # Run Forest
        params_dict = tracker.params_dict()
        tracker.params_dict_cache = json.dumps(params_dict, cls=DjangoJSONEncoder)
        tracker.save(update_fields=["params_dict_cache"])
        TREE_TO_FOREST_FUNCTION[tracker.forest_tree](**params_dict)
        
        # Save data
        tracker.forest_output_exists = tracker.construct_summary_statistics()
        tracker.save(update_fields=["forest_output_exists"])
        save_cached_files(tracker)
    except Exception:
        tracker.status = tracker.Status.error
        tracker.stacktrace = traceback.format_exc()
    else:
        tracker.status = tracker.Status.success
    tracker.clean_up_files()
    tracker.process_end_time = timezone.now()
    tracker.save()
    


def create_local_data_files(tracker, chunks):
    for chunk in chunks.values("study__object_id", *chunk_fields):
        contents = s3_retrieve(chunk["chunk_path"], chunk["study__object_id"], raw_path=True)
        file_name = os.path.join(
            tracker.data_input_path,
            determine_file_name(chunk),
        )
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, "xb") as f:
            f.write(contents)


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


def save_cached_files(tracker):
    if os.path.exists(tracker.all_bv_set_path):
        with open(tracker.all_bv_set_path, "rb") as f:
            tracker.save_all_bv_set_bytes(f.read())
    if os.path.exists(tracker.all_memory_dict_path):
        with open(tracker.all_memory_dict_path, "rb") as f:
            tracker.save_all_memory_dict_bytes(f.read())
