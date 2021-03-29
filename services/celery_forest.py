import json
import os
import shutil
import traceback
from datetime import datetime, timedelta

from cronutils.error_handler import NullErrorHandler
from django.db import DatabaseError, transaction
from django.db.models import Sum
from django.utils import timezone
from temp_forest_repo.forest.forest.jasmine.traj2stats import gps_stats_main as jasmine_main
from temp_forest_repo.forest.forest.willow.log_stats import log_stats_main as willow_main
from kombu.exceptions import OperationalError

from config.constants import FOREST_QUEUE
from database.data_access_models import ChunkRegistry
from database.tableau_api_models import ForestTracker
from libs.celery_control import forest_celery_app
from libs.forest_integration.forest_data_interpretation import construct_summary_statistics


DATA_FOLDER = os.path.join(os.getcwd(), "forest_data_files")


# run via cron every five minutes
def create_forest_celery_tasks():
    pending = ForestTracker.objects.filter(status=ForestTracker.Status.QUEUED)
    # we reuse the high level strategy from data processing celery tasks, see that documentation.
    expiry = (datetime.utcnow() + timedelta(minutes=5)).replace(second=30, microsecond=0)
    now = timezone.now()

    # with make_error_sentry(sentry_type=SentryTypes.data_processing):  # add a new type?
    with NullErrorHandler():  # for debugging, does not suppress errors
        for tracker in pending:
            print(f"Queueing up celery task for {tracker.participant} on tree {tracker.forest_tree} from {tracker.data_date_start} to {tracker.data_date_end}")
            enque_forest_task(
                args=[tracker.id],
                max_retries=0,
                expires=expiry,
                task_track_started=True,
                task_publish_retry=False,
                retry=False,
            )


#run via celery as long as tasks exist
@forest_celery_app.task(queue=FOREST_QUEUE)
def celery_run_forest(forest_tracker_id):
    with transaction.atomic():
        tracker = ForestTracker.objects.filter(id=forest_tracker_id).first()

        participant = tracker.participant
        forest_tree = tracker.forest_tree
        trackers = ForestTracker.objects.select_for_update().filter(participant=participant, forest_tree=forest_tree)
        tracker = trackers.filter(status=ForestTracker.Status.QUEUED).order_by(
            "-data_date_start").first()
        if tracker is None:
            return
        if trackers.filter(status=ForestTracker.Status.RUNNING).exists():
            enque_forest_task(**tracker)
            return
        tracker.status = ForestTracker.Status.RUNNING
        tracker.process_start_time = timezone.now()
        tracker.save(update_fields=["status", "process_start_time"])

    data = ChunkRegistry.objects.filter(participant=participant)
    tracker.file_size = data.aggregate(Sum('file_size')).get('file_size__sum')
    print(f"collecting data. running task from celery on tracker {tracker.id}")
    try:
        io_folder, input_data_folder, output_data_folder = create_local_data_files(tracker, data)
        tracker.process_download_end_time = timezone.now()
        params = {
            'study_folder': input_data_folder,
            'output_folder': output_data_folder,
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
        construct_summary_statistics(tracker, tracker.participant.study, tracker.participant,
                                     tracker.forest_tree, forest_output)
        clean_local_data_files(io_folder)
    except Exception:
        clean_local_data_files(os.path.join(DATA_FOLDER, str(tracker.external_id)))
        tracker.status = tracker.Status.ERROR
        tracker.stacktrace = traceback.format_exc()
        tracker.process_end_time = timezone.now()
        tracker.save()
        return

    tracker.status = tracker.Status.SUCCESS
    tracker.process_end_time = timezone.now()
    tracker.save()


def create_local_data_files(tracker, data):
    io_folder = os.path.join(DATA_FOLDER, str(tracker.external_id))
    input_data_folder = os.path.join(io_folder, "data")
    output_data_folder = os.path.join(io_folder, "output")
    os.mkdir(io_folder)
    os.mkdir(input_data_folder)
    os.mkdir(output_data_folder)
    for data_file in data:
        file_name = os.path.join(input_data_folder, "".join(filter(lambda c: c != "/", data_file.chunk_hash)) + ".txt")
        contents = data_file.s3_retrieve()
        with open(file_name, "x") as f:
            f.write(contents.decode("utf-8"))
    return io_folder, input_data_folder, output_data_folder


def clean_local_data_files(io_folder):
    shutil.rmtree(io_folder)  # this is equivalent to rm -r, double check any changes made here


def enque_forest_task(*args, **kwargs):
    for i in range(10):
        try:
            return celery_run_forest.apply_async(*args, **kwargs)
        except OperationalError:
            if i < 3:
                pass
            else:
                raise
