from datetime import datetime, timedelta, timezone

from config.constants import FOREST_QUEUE
from database.tableau_api_models import ForestTracker
from libs.celery_control import forest_celery_app
from libs.forest_integration.forest_data_interpretation import construct_summary_statistics

from libs.sentry import make_error_sentry, SentryTypes
from services.celery_push_notifications import safe_queue_push

# run via cron every five minutes
def create_forest_celery_tasks():
    pending = ForestTracker.objects.filter(status=ForestTracker.Status.QUEUED)
    # we reuse the high level strategy from data processing celery tasks, see that documentation.
    expiry = (datetime.utcnow() + timedelta(minutes=5)).replace(second=30, microsecond=0)
    now = timezone.now()

    with make_error_sentry(sentry_type=SentryTypes.data_processing):  # add a new type?
        # surveys and schedules are guaranteed to have the same keys, assembling the data structures
        # is a pain, so it is factored out. sorry, but not sorry. it was a mess.
        for tracker in pending:
            print(f"Queueing up celery task for {tracker.participant} on tree {tracker.forest_tree} from {tracker.data_date_start} to {tracker.data_date_end}")
            safe_queue_push(
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

    # actually run forest here
    forest_output = []

    construct_summary_statistics(tracker.participant.study, tracker.participant, tracker.forest_tree, forest_output)