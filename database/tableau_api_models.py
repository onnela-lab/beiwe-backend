import json
import os
import shutil
import uuid

from django.db import models
from database.common_models import TimestampedModel
from database.user_models import Participant
from libs.forest_integration.constants import ForestTree


class SummaryStatisticDaily(TimestampedModel):
    participant = models.ForeignKey(Participant, on_delete=models.CASCADE)
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

    jasmine_tracker = models.ForeignKey("ForestMetadata", null=True, on_delete=models.PROTECT, related_name="jasmine_summary_statistics")
    willow_tracker = models.ForeignKey("ForestMetadata", null=True, on_delete=models.PROTECT, related_name="willow_summary_statistics")


class ForestTracker(TimestampedModel):
    participant = models.ForeignKey(
        'Participant', on_delete=models.PROTECT, db_index=True
    )
    # the external id is used for endpoints that refer to forest trackers to avoid exposing the
    # primary keys of the model. it is intentionally not the primary key
    external_id = models.UUIDField(default=uuid.uuid4, editable=False)
    
    metadata = models.ForeignKey("ForestMetadata", on_delete=models.PROTECT)

    forest_tree = models.TextField(choices=ForestTree.choices())
    data_date_start = models.DateField()  # inclusive
    data_date_end = models.DateField()  # inclusive

    total_file_size = models.IntegerField(blank=True, null=True)  # input file size sum for accounting
    process_start_time = models.DateTimeField(null=True, blank=True)
    process_download_end_time = models.DateField(null=True, blank=True)
    process_end_time = models.DateTimeField(null=True, blank=True)

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
    forest_version = models.CharField(blank=True, max_length=10)
    
    def clean_up_files(self):
        """
        Delete temporary input and output files from this Forest run.
        """
        shutil.rmtree(self.data_base_path)
    
    @property
    def data_base_path(self):
        """
        Return the path to the base data folder, creating it if it doesn't already exist.
        """
        return os.path.join("/tmp", str(self.external_id), self.forest_tree)
    
    @property
    def data_input_path(self):
        """
        Return the path to the input data folder, creating it if it doesn't already exist.
        """
        return os.path.join(self.data_base_path, "data")
    
    @property
    def data_output_path(self):
        """
        Return the path to the output data folder, creating it if it doesn't already exist.
        """
        return os.path.join(self.data_base_path, "output")
    
    def forest_params(self):
        """
        Return a dict of params to pass into the forest function.
        """
        params = {
            "output_folder": self.data_output_path,
            "study_folder": self.data_input_path,
            "time_end": self.data_date_end,
            "time_start": self.data_date_start,
        }
        return {**self.metadata.metadata_for_tree(self.forest_tree), **params}
    
    def get_slug(self):
        """
        Return a human-readable identifier.
        """
        parts = [
            "data",
            self.participant.patient_id,
            self.forest_tree,
            str(self.data_date_start),
            str(self.data_date_end),
        ]
        return "_".join(parts)


class ForestMetadata(TimestampedModel):
    """
    Model for tracking metadata used in Forest analyses. There is one object for all trees.
    
    When adding support for a new tree, make sure to add a migration to populate existing
    ForestMetadata objects with the default metadata for the new tree. This way, all existing
    ForestTrackers are still associated to the same ForestMetadata object and we don't have to give
    a warning to users that the metadata have changed.
    """
    # Note: making a NullBooleanField unique=True allows us to ensure only one object can have
    #       default=True at any time (null is considered unique). This means this field should be
    #       consumed as True or falsy (null is false), as the value should never be actually set to
    #       `False`.
    default = models.NullBooleanField(unique=True)
    notes = models.TextField(blank=True)
    name = models.TextField(blank=True)
    
    jasmine_json_string = models.TextField()
    willow_json_string = models.TextField()
    
    def metadata_for_tree(self, tree_name):
        if tree_name not in ForestTree.values():
            raise KeyError(f"Invalid tree \"{tree_name}\". Must be one of {ForestTree.values()}.")
        json_string_field_name = f"{tree_name}_json_string"
        return json.loads(getattr(self, json_string_field_name))
