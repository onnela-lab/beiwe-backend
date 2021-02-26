import uuid

from django.db import models
from database.common_models import TimestampedModel
from database.study_models import Study
from database.user_models import Participant


class SummaryStatisticDaily(TimestampedModel):
    participant = models.ForeignKey(Participant, on_delete=models.CASCADE)
    study = models.ForeignKey(Study, on_delete=models.CASCADE)
    date = models.DateField(db_index=True)
    distance_diameter = models.IntegerField(null=True, blank=True)
    distance_from_home = models.IntegerField(null=True, blank=True)
    distance_traveled = models.IntegerField(null=True, blank=True)
    flight_distance_average = models.IntegerField(null=True, blank=True)
    flight_distance_standard_deviation = models.IntegerField(null=True, blank=True)
    flight_duration_average = models.IntegerField(null=True, blank=True)
    flight_duration_standard_deviation = models.IntegerField(null=True, blank=True)
    gps_data_missing_duration = models.IntegerField(null=True, blank=True)
    home_duration = models.IntegerField(null=True, blank=True)
    physical_circadian_rhythm = models.FloatField(null=True, blank=True)
    physical_circadian_rhythm_stratified = models.FloatField(null=True, blank=True)
    radius_of_gyration = models.IntegerField(null=True, blank=True)
    significant_location_count = models.IntegerField(null=True, blank=True)
    significant_location_entropy = models.IntegerField(null=True, blank=True)
    stationary_fraction = models.TextField(null=True, blank=True)
    text_incoming_count = models.IntegerField(null=True, blank=True)
    text_incoming_degree = models.IntegerField(null=True, blank=True)
    text_incoming_length = models.IntegerField(null=True, blank=True)
    text_incoming_responsiveness = models.IntegerField(null=True, blank=True)
    text_outgoing_count = models.IntegerField(null=True, blank=True)
    text_outgoing_degree = models.IntegerField(null=True, blank=True)
    text_outgoing_length = models.IntegerField(null=True, blank=True)
    text_reciprocity = models.IntegerField(null=True, blank=True)
    call_incoming_count = models.IntegerField(null=True, blank=True)
    call_incoming_degree = models.IntegerField(null=True, blank=True)
    call_incoming_duration = models.IntegerField(null=True, blank=True)
    call_incoming_responsiveness = models.IntegerField(null=True, blank=True)
    call_outgoing_count = models.IntegerField(null=True, blank=True)
    call_outgoing_degree = models.IntegerField(null=True, blank=True)
    call_outgoing_duration = models.IntegerField(null=True, blank=True)
    acceleration_direction = models.TextField(null=True, blank=True)
    accelerometer_coverage_fraction = models.TextField(null=True, blank=True)
    accelerometer_signal_variability = models.TextField(null=True, blank=True)
    accelerometer_univariate_summaries = models.FloatField(null=True, blank=True)
    device_proximity = models.BooleanField(null=True, blank=True)
    total_power_events = models.IntegerField(null=True, blank=True)
    total_screen_events = models.IntegerField(null=True, blank=True)
    total_unlock_events = models.IntegerField(null=True, blank=True)
    awake_onset_time = models.DateTimeField(null=True, blank=True)
    sleep_duration = models.IntegerField(null=True, blank=True)
    sleep_onset_time = models.DateTimeField(null=True, blank=True)

class ForestTracker(TimestampedModel):
    participant = models.ForeignKey(
        'Participant', on_delete=models.PROTECT, db_index=True
    )
    external_id = models.UUIDField(default=uuid.uuid4, editable=False)
    # the external id is used for endpoints that refer to forest trackers to avoid exposing the
    # primary keys of the model. it is intentionally not the primary key

    forest_tree = models.TextField()
    data_date_start = models.DateField()  # inclusive
    data_date_end = models.DateField()  # inclusive

    file_size = models.IntegerField()  # input file size sum for accounting
    process_start_time = models.DateTimeField(null=True, blank=True)
    process_end_time = models.DateTimeField(null=True, blank=True)
    # celery_task_id?
    # time limit?

    class Status:
        QUEUED = 'Queued'
        RUNNING = 'Running'
        SUCCESS = 'Success'
        ERROR = 'Error'
        CANCELLED = 'Cancelled'

    STATUS_CHOICES = (
        (Status.QUEUED, Status.QUEUED),
        (Status.RUNNING, Status.RUNNING),
        (Status.SUCCESS, Status.SUCCESS),
        (Status.ERROR, Status.ERROR),
        (Status.CANCELLED, Status.CANCELLED),
    )
    status = models.TextField(choices=STATUS_CHOICES)
    stacktrace = models.TextField(null=True, blank=True, default=None)  # for logs
    forest_version = models.CharField(max_length=10)
    commit_hash = models.CharField(max_length=40)
    metadata = models.TextField()  # json string, add validator?
    metadata_hash = models.CharField(max_length=64)


# class ForestTree(TimestampedModel):
#     name = models.CharField(max_length=30)
#
