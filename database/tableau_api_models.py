import csv
import datetime
import json
import os
import pickle
import shutil
import uuid

from django.db import models
from database.common_models import TimestampedModel
from database.user_models import Participant
from libs.forest_integration.constants import ForestTree, TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS
from libs.utils.date_utils import datetime_to_list


class ForestParam(TimestampedModel):
    """
    Model for tracking params used in Forest analyses. There is one object for all trees.
    
    When adding support for a new tree, make sure to add a migration to populate existing
    ForestMetadata objects with the default metadata for the new tree. This way, all existing
    ForestTasks are still associated to the same ForestMetadata object and we don't have to give
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
    
    def params_for_tree(self, tree_name):
        if tree_name not in ForestTree.values():
            raise KeyError(f"Invalid tree \"{tree_name}\". Must be one of {ForestTree.values()}.")
        json_string_field_name = f"{tree_name}_json_string"
        return json.loads(getattr(self, json_string_field_name))


class ForestTask(TimestampedModel):
    participant = models.ForeignKey(
        'Participant', on_delete=models.PROTECT, db_index=True
    )
    # the external id is used for endpoints that refer to forest trackers to avoid exposing the
    # primary keys of the model. it is intentionally not the primary key
    external_id = models.UUIDField(default=uuid.uuid4, editable=False)
    
    forest_param = models.ForeignKey(ForestParam, on_delete=models.PROTECT)
    params_dict_cache = models.TextField(blank=True)  # Cache of the params used
    
    forest_tree = models.TextField(choices=ForestTree.choices())
    data_date_start = models.DateField()  # inclusive
    data_date_end = models.DateField()  # inclusive
    
    total_file_size = models.BigIntegerField(blank=True, null=True)  # input file size sum for accounting
    process_start_time = models.DateTimeField(null=True, blank=True)
    process_download_end_time = models.DateTimeField(null=True, blank=True)
    process_end_time = models.DateTimeField(null=True, blank=True)
    
    # Whether or not there was any data output by Forest (None indicates unknown)
    forest_output_exists = models.NullBooleanField()
    
    class Status:
        queued = 'queued'
        running = 'running'
        success = 'success'
        error = 'error'
        cancelled = 'cancelled'
        
        @classmethod
        def choices(cls):
            return [(choice, choice.title()) for choice in cls.values()]
        
        @classmethod
        def values(cls):
            return [cls.queued, cls.running, cls.success, cls.error, cls.cancelled]
    
    status = models.TextField(choices=Status.choices())
    stacktrace = models.TextField(null=True, blank=True, default=None)  # for logs
    forest_version = models.CharField(blank=True, max_length=10)
    
    all_bv_set_s3_key = models.TextField(blank=True)
    all_memory_dict_s3_key = models.TextField(blank=True)

    @property
    def all_bv_set_path(self):
        return os.path.join(self.data_output_path, "all_BV_set.pkl")
    
    @property
    def all_memory_dict_path(self):
        return os.path.join(self.data_output_path, "all_memory_dict.pkl")

    def construct_summary_statistics(self):
        """
        Construct summary statistics from forest output, returning whether or not any
        SummaryStatisticDaily has potentially been created or updated.
        """
        if not os.path.exists(self.forest_results_path):
            return False

        if self.forest_tree == ForestTree.jasmine:
            task_attribute = "jasmine_task"
        elif self.forest_tree == ForestTree.willow:
            task_attribute = "willow_task"
        else:
            raise Exception("Unknown tree")
        
        with open(self.forest_results_path, "r") as f:
            reader = csv.DictReader(f)
            has_data = False
    
            for line in reader:
                has_data = True
                summary_date = datetime.date(
                    int(float(line['year'])),
                    int(float(line['month'])),
                    int(float(line['day'])),
                )
                if not (self.data_date_start < summary_date < self.data_date_end):
                    continue
                updates = {}
                for column_name, value in line.items():
                    if (self.forest_tree, column_name) in TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS:
                        summary_stat_field, interpretation_function = TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS[(self.forest_tree, column_name)]
                        if interpretation_function is not None:
                            updates[summary_stat_field] = interpretation_function(value, line)
                        elif value == '':
                            updates[summary_stat_field] = None
                        else:
                            updates[summary_stat_field] = value
            
                updates[task_attribute] = self
                
                data = {
                    "date": summary_date,
                    "defaults": updates,
                    "participant": self.participant,
                }
                SummaryStatisticDaily.objects.update_or_create(**data)
        return has_data

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
    
    @property
    def forest_results_path(self):
        """
        Return the path to the file that contains the output of Forest.
        """
        return os.path.join(self.data_output_path, f"{self.participant.patient_id}.csv")
    
    def params_dict(self):
        """
        Return a dict of params to pass into the Forest function.
        """
        other_params = {
            "output_folder": self.data_output_path,
            "study_folder": self.data_input_path,
            # Need to add a day since this model tracks time end inclusively, but Forest expects
            # it exclusively
            "time_end": datetime_to_list(self.data_date_end + datetime.timedelta(days=1)),
            "time_start": datetime_to_list(self.data_date_start),
        }
        if self.forest_tree == ForestTree.jasmine:
            other_params["all_BV_set"] = self.get_all_bv_set_dict()
            other_params["all_memory_dict"] = self.get_all_memory_dict_dict()
        return {**self.forest_param.params_for_tree(self.forest_tree), **other_params}
    
    def generate_all_bv_set_s3_key(self):
        """
        Generate the S3 key that all_bv_set_s3_key should live in (whereas the direct
        all_bv_set_s3_key field on the instance is where it currently lives, regardless
        of how generation changes).
        """
        return os.path.join(self.s3_base_folder, 'all_bv_set.pkl')
    
    def generate_all_memory_dict_s3_key(self):
        """
        Generate the S3 key that all_memory_dict_s3_key should live in (whereas the direct
        all_memory_dict_s3_key field on the instance is where it currently lives, regardless
        of how generation changes).
        """
        return os.path.join(self.s3_base_folder, 'all_memory_dict.pkl')
    
    def get_all_bv_set_dict(self):
        """
        Return the unpickled all_bv_set dict.
        """
        if not self.all_bv_set_s3_key:
            return None  # Forest expects None if it doesn't exist
        from libs.s3 import s3_retrieve
        bytes = s3_retrieve(self.all_bv_set_s3_key, self.participant.study.object_id, raw_path=True)
        return pickle.loads(bytes)
    
    def get_all_memory_dict_dict(self):
        """
        Return the unpickled all_memory_dict dict.
        """
        if not self.all_memory_dict_s3_key:
            return None  # Forest expects None if it doesn't exist
        from libs.s3 import s3_retrieve
        bytes = s3_retrieve(
            self.all_memory_dict_s3_key,
            self.participant.study.object_id,
            raw_path=True,
        )
        return pickle.loads(bytes)
    
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
    
    @property
    def s3_base_folder(self):
        return os.path.join(self.participant.study.object_id, "forest")
    
    def save_all_bv_set_bytes(self, all_bv_set_bytes):
        from libs.s3 import s3_upload
        s3_upload(
            self.generate_all_bv_set_s3_key(),
            all_bv_set_bytes,
            self.participant.study.object_id,
            raw_path=True,
        )
        self.all_bv_set_s3_key = self.generate_all_bv_set_s3_key()
        self.save(update_fields=["all_bv_set_s3_key"])
    
    def save_all_memory_dict_bytes(self, all_memory_dict_bytes):
        from libs.s3 import s3_upload
        s3_upload(
            self.generate_all_memory_dict_s3_key(),
            all_memory_dict_bytes,
            self.participant.study.object_id,
            raw_path=True,
        )
        self.all_memory_dict_s3_key = self.generate_all_memory_dict_s3_key()
        self.save(update_fields=["all_memory_dict_s3_key"])


class SummaryStatisticDaily(TimestampedModel):
    participant = models.ForeignKey(Participant, on_delete=models.CASCADE)
    date = models.DateField(db_index=True)
    
    # GPS
    distance_diameter = models.FloatField(null=True, blank=True)
    distance_from_home = models.FloatField(null=True, blank=True)
    distance_traveled = models.FloatField(null=True, blank=True)
    flight_distance_average = models.FloatField(null=True, blank=True)
    flight_distance_standard_deviation = models.FloatField(null=True, blank=True)
    flight_duration_average = models.FloatField(null=True, blank=True)
    flight_duration_standard_deviation = models.FloatField(null=True, blank=True)
    gps_data_missing_duration = models.IntegerField(null=True, blank=True)
    home_duration = models.FloatField(null=True, blank=True)
    physical_circadian_rhythm = models.FloatField(null=True, blank=True)
    physical_circadian_rhythm_stratified = models.FloatField(null=True, blank=True)
    radius_of_gyration = models.IntegerField(null=True, blank=True)
    significant_location_count = models.IntegerField(null=True, blank=True)
    significant_location_entropy = models.IntegerField(null=True, blank=True)
    stationary_fraction = models.TextField(null=True, blank=True)
    
    # Texts
    text_incoming_count = models.IntegerField(null=True, blank=True)
    text_incoming_degree = models.IntegerField(null=True, blank=True)
    text_incoming_length = models.IntegerField(null=True, blank=True)
    text_incoming_responsiveness = models.IntegerField(null=True, blank=True)
    text_outgoing_count = models.IntegerField(null=True, blank=True)
    text_outgoing_degree = models.IntegerField(null=True, blank=True)
    text_outgoing_length = models.IntegerField(null=True, blank=True)
    text_reciprocity = models.IntegerField(null=True, blank=True)
    
    # Calls
    call_incoming_count = models.IntegerField(null=True, blank=True)
    call_incoming_degree = models.IntegerField(null=True, blank=True)
    call_incoming_duration = models.IntegerField(null=True, blank=True)
    call_incoming_responsiveness = models.IntegerField(null=True, blank=True)
    call_outgoing_count = models.IntegerField(null=True, blank=True)
    call_outgoing_degree = models.IntegerField(null=True, blank=True)
    call_outgoing_duration = models.IntegerField(null=True, blank=True)
    
    # Accelerometer
    acceleration_direction = models.TextField(null=True, blank=True)
    accelerometer_coverage_fraction = models.TextField(null=True, blank=True)
    accelerometer_signal_variability = models.TextField(null=True, blank=True)
    accelerometer_univariate_summaries = models.FloatField(null=True, blank=True)
    device_proximity = models.BooleanField(null=True, blank=True)
    
    # Power state
    total_power_events = models.IntegerField(null=True, blank=True)
    total_screen_events = models.IntegerField(null=True, blank=True)
    total_unlock_events = models.IntegerField(null=True, blank=True)
    
    # Multiple domains
    awake_onset_time = models.DateTimeField(null=True, blank=True)
    sleep_duration = models.IntegerField(null=True, blank=True)
    sleep_onset_time = models.DateTimeField(null=True, blank=True)

    jasmine_task = models.ForeignKey(ForestTask, blank=True, null=True, on_delete=models.PROTECT, related_name="jasmine_summary_statistics")
    willow_task = models.ForeignKey(ForestTask, blank=True, null=True, on_delete=models.PROTECT, related_name="willow_summary_statistics")

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['date', 'participant'], name="unique_summary_statistic")
        ]
