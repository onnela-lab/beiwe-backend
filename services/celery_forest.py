from datetime import datetime, timedelta

from cronutils.error_handler import NullErrorHandler
from django.utils import timezone
from kombu.exceptions import OperationalError

from config.constants import FOREST_QUEUE
from database.tableau_api_models import ForestTracker
from libs.celery_control import forest_celery_app
from libs.forest_integration.forest_data_interpretation import construct_summary_statistics

from libs.sentry import make_error_sentry, SentryTypes

# run via cron every five minutes
def create_forest_celery_tasks():
    pending = ForestTracker.objects.filter(status=ForestTracker.Status.QUEUED)
    # we reuse the high level strategy from data processing celery tasks, see that documentation.
    expiry = (datetime.utcnow() + timedelta(minutes=5)).replace(second=30, microsecond=0)
    now = timezone.now()

    # with make_error_sentry(sentry_type=SentryTypes.data_processing):  # add a new type?
    with NullErrorHandler:
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
    tracker = ForestTracker.objects.get(id=forest_tracker_id)
    # try to finder earlier tracker?
    # mutex operation?
    print("running task from celery...")
    tracker.status = tracker.Status.RUNNING
    tracker.process_start_time = timezone.now()
    try:
        # actually run forest here
        forest_output = ''
        construct_summary_statistics(tracker.participant.study, tracker.participant, tracker.forest_tree, forest_output)
    except Exception as exception:
        tracker.status = tracker.Status.ERROR
        tracker.stacktrace = str(exception)
    tracker.status = tracker.Status.SUCCESS
    tracker.process_end_time = timezone.now()


def enque_forest_task(*args, **kwargs):
    for i in range(10):
        try:
            return forest_celery_app.apply_async(*args, **kwargs)
        except OperationalError:
            if i < 3:
                pass
            else:
                raise
