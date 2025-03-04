from __future__ import annotations

import pickle
import uuid
from datetime import timedelta
from os.path import join as path_join
from typing import Dict

from django.db import models
from django.db.models import Manager

from config.settings import DOMAIN_NAME
from constants.celery_constants import ForestTaskStatus
from constants.forest_constants import (DEFAULT_FOREST_PARAMETERS, FOREST_PICKLING_ERROR,
                                        ForestTree, NON_PICKLED_PARAMETERS, OAK_DATE_FORMAT_PARAMETER,
                                        PARAMETER_ALL_BV_SET,
                                        PARAMETER_ALL_MEMORY_DICT, PARAMETER_CONFIG_PATH,
                                        PARAMETER_INTERVENTIONS_FILEPATH,
                                        ROOT_FOREST_TASK_PATH, SYCAMORE_DATE_FORMAT)
from database.common_models import TimestampedModel
from database.user_models_participant import Participant
from libs.utils.date_utils import datetime_to_list
from libs.utils.forest_utils import get_jasmine_all_bv_set_dict, get_jasmine_all_memory_dict_dict


#
## GO READ THE MULTILINE STATEMENT AT THE TOP OF services/celery_forest.py
#

class ForestTask(TimestampedModel):
    # All forest tasks are defined to be associated with a single participant
    participant: Participant = models.ForeignKey('Participant', on_delete=models.PROTECT, db_index=True)
    
    forest_tree = models.TextField(choices=ForestTree.choices())
    forest_version = models.CharField(blank=True, max_length=10, null=False, default="")
    forest_commit = models.CharField(blank=True, max_length=40, null=False, default="")
    
    # the external id is used for endpoints that refer to forest trackers to avoid exposing the
    # primary keys of the model. it is intentionally not the primary key
    external_id = models.UUIDField(default=uuid.uuid4, editable=False)
    
    # due to code churn we pickle parameters that are passed to forest.
    pickled_parameters = models.BinaryField(blank=True, null=True)
    # all forest tasks run on a data range, (these are parameters entered at creation from the web)
    data_date_start = models.DateField()  # inclusive
    data_date_end = models.DateField()  # inclusive
    
    # runtime records
    total_file_size = models.BigIntegerField(blank=True, null=True)  # input file size sum for accounting
    process_start_time = models.DateTimeField(null=True, blank=True)
    process_download_end_time = models.DateTimeField(null=True, blank=True)
    process_end_time = models.DateTimeField(null=True, blank=True)
    status = models.TextField(choices=ForestTaskStatus.choices())
    stacktrace = models.TextField(null=True, blank=True, default=None)
    # Whether or not there was any data output by Forest (None means construct_summary_statistics errored)
    forest_output_exists = models.BooleanField(null=True, blank=True)
    
    # S3 file paths
    output_zip_s3_path = models.TextField(blank=True)  # includes study folder in path
    # Jasmine has special parameters, these are their s3 file paths.
    all_bv_set_s3_key = models.TextField(blank=True)
    all_memory_dict_s3_key = models.TextField(blank=True)
    
    # related field typings (IDE halp)
    jasmine_summary_statistics: Manager[SummaryStatisticDaily]
    sycamore_summary_statistics: Manager[SummaryStatisticDaily]
    willow_summary_statistics: Manager[SummaryStatisticDaily]
    oak_summary_statistics: Manager[SummaryStatisticDaily]
    
    @property
    def taskname(self) -> str:
        # this is the Foreign key reference field name in SummaryStatisticDaily
        return self.forest_tree + "_task"
    
    @property
    def sentry_tags(self) -> Dict[str, str]:
        from libs.utils.http_utils import easy_url
        url = path_join(DOMAIN_NAME, easy_url("forest_endpoints.task_log", study_id=self.participant.study.id))
        return {
            "participant": self.participant.patient_id,
            "study": self.participant.study.name,
            "forest_tree": self.forest_tree,
            "forest_version": self.forest_version,
            "forest_commit": self.forest_commit,
            "external_id": self.external_id,
            "status": self.status if self.status else "None",
            "task_page": url,
            # "pickled_parameters": self.pickled_parameters,
            "total_file_size": str(self.total_file_size),
            "data_date_start": self.data_date_start.isoformat() if self.data_date_start else "None",
            "data_date_end": self.data_date_end.isoformat() if self.data_date_end else "None",
            "process_start_time": self.process_start_time.isoformat() if self.process_start_time else "None",
            "process_download_end_time": self.process_download_end_time.isoformat() if self.process_download_end_time else "None",
            "process_end_time": self.process_end_time.isoformat() if self.process_end_time else "None",
            # "stacktrace": self.stacktrace, # it just doesn't work
            # "forest_output_exists": self.forest_output_exists,
            # "output_zip_s3_path": self.output_zip_s3_path,
            # "all_bv_set_s3_key": self.all_bv_set_s3_key,
            # "all_memory_dict_s3_key": self.all_memory_dict_s3_key,
        }
    
    #
    ## forest tree parameters
    #
    def get_params_dict(self) -> dict:
        """ Return a dict of params to pass into the Forest function. The task flag is used to
        indicate whether this is being called for use in the serializer or for use in a task (in
        which case we can call additional functions as needed). """
        # Every tree expects the two folder paths and the time zone string.
        # Note: the tz_string may (intentionally) be overwritten by the unpickled parameters.)
        params = {
            "output_folder": self.data_output_path, "study_folder": self.data_input_path,
            "tz_str": self.participant.study.timezone_name,
        }
        
        # get the parameters that were used originally on this task, which may differ from the
        # defaults (due to code drift, we don't currently have a way to change them)
        if self.pickled_parameters:
            # unpickling specifically avoids the output and study folder parameters
            params.update(self.unpickle_from_pickled_parameters())
        else:
            params.update(DEFAULT_FOREST_PARAMETERS[self.forest_tree])
        
        self.handle_tree_specific_params(params)
        return params
    
    def pickle_to_pickled_parameters(self, parameters: Dict):
        """ takes parameters and pickles them """
        if not isinstance(parameters, dict):
            raise TypeError("parameters must be a dict")
        # we need to clear but certain parameters, but we don't want to mutate the dictionary
        cleaned_parameters = parameters.copy()
        for parameter in NON_PICKLED_PARAMETERS:
            cleaned_parameters.pop(parameter, None)
        self.pickled_parameters = pickle.dumps(cleaned_parameters)
        self.save()
    
    def unpickle_from_pickled_parameters(self) -> Dict:
        """ Unpickle the pickled_parameters field. """
        # If you see a stacktrace pointing here that means Forest code changed substantially and
        # this Forest task's code fundamentally change in a way that means it cannot be rerun.
        if self.pickled_parameters:
            try:
                ret = pickle.loads(self.pickled_parameters)
            except Exception:
                raise ValueError(FOREST_PICKLING_ERROR)
            # we need to return something that can be im(mediately unpacked into a dict.
            # None is returned when it is empty.  Empty (byte)string should be impossible.
            if ret is None:
                return {}
            if not isinstance(ret, dict):
                raise TypeError(f"unpickled parameters must be a dict, found {type(ret)}")
            return ret
        return {}
    
    def safe_unpickle_parameters_as_string(self) -> str:
        # it is common that we want a string representation of the parameters, but we need to handle
        # pickling errors under that scenario.
        try:
            return repr(self.unpickle_from_pickled_parameters())  # use repr
        except Exception as e:
            return str(e)
    
    def handle_tree_specific_params(self, params: Dict):
        self.handle_tree_specific_date_params(params)
        if self.forest_tree == ForestTree.jasmine:
            self.assemble_jasmine_dynamic_params(params)
        if self.forest_tree == ForestTree.sycamore:
            self.assemble_sycamore_folder_path_params(params)
    
    # TODO: forest uses date components/strings because previously we did not pickle the parameters.
    def handle_tree_specific_date_params(self, params: dict):
        # We need to add a day, this model tracks time end inclusively, but Forest expects it
        # exclusively
        
        if self.forest_tree == ForestTree.sycamore:
            # sycamore expects "time_end" and "time_start" as strings in the format "YYYY-MM-DD"
            params.update({
                "start_date": self.data_date_start.strftime(SYCAMORE_DATE_FORMAT),
                "end_date": (self.data_date_end + timedelta(days=1)).strftime(SYCAMORE_DATE_FORMAT),
            })
        elif self.forest_tree == ForestTree.oak:
            # oak expects "time_end" and "time_start" as strings in the format "YYYY-MM-DD HH_MM_SS"
            params.update({
                "time_start": self.data_date_start.strftime(OAK_DATE_FORMAT_PARAMETER),
                "time_end": (self.data_date_end + timedelta(days=1)).strftime(OAK_DATE_FORMAT_PARAMETER),
            })
        else:
            # other trees expect lists of datetime parameters.
            params.update({"time_start": datetime_to_list(self.data_date_start),
                           "time_end": datetime_to_list(self.data_date_end + timedelta(days=1))})
    
    def assemble_jasmine_dynamic_params(self, params: dict):
        """ real code is in libs/forest_utils.py """
        params[PARAMETER_ALL_BV_SET] = get_jasmine_all_bv_set_dict(self)
        params[PARAMETER_ALL_MEMORY_DICT] = get_jasmine_all_memory_dict_dict(self)
    
    def assemble_sycamore_folder_path_params(self, params: dict):
        """ Sycamore has some extra files and file paths """
        params[PARAMETER_CONFIG_PATH] = self.study_config_path
        params[PARAMETER_INTERVENTIONS_FILEPATH] = self.interventions_filepath
    
    #
    ## File paths
    #
    @property
    def root_path_for_task(self):
        """ The uuid-folder name for this task. /tmp/forest/<uuid> """
        return path_join(ROOT_FOREST_TASK_PATH, str(self.external_id))
    
    @property
    def tree_base_path(self):
        """ Path to the base data for this task's tree. /tmp/forest/<uuid>/<tree> """
        return path_join(self.root_path_for_task, self.forest_tree)
    
    @property
    def data_input_path(self) -> str:
        """ Path to the input data folder. /tmp/forest/<uuid>/<tree>/data """
        return path_join(self.tree_base_path, "data")
    
    @property
    def data_output_path(self) -> str:
        """ Path to the output data folder. /tmp/forest/<uuid>/<tree>/output """
        return path_join(self.tree_base_path, "output")
    
    @property
    def task_report_path(self) -> str:
        """ Path to the task report file. /tmp/forest/<uuid>/<tree>/output/task_report.txt """
        return path_join(self.data_output_path, "task_report.txt")
    
    @property
    def forest_results_path(self) -> str:
        """ Path to the file that contains the output of Forest.
        /tmp/forest/<uuid>/<tree>/output/daily/<patient_id>.csv
        Beiwe ONLY collects for streaming the daily summaries. """
        return path_join(self.data_output_path, "daily", f"{self.participant.patient_id}.csv")
    
    @property
    def interventions_filepath(self) -> str:
        """ The study interventions file path for the participant's survey data.
         /tmp/forest/<uuid>/<tree>/<study_objectid>_interventions.json """
        filename = self.participant.study.object_id + "_interventions.json"
        return path_join(self.tree_base_path, filename)
    
    @property
    def study_config_path(self) -> str:
        """ The study configuration file file path.
        /tmp/forest/<uuid>/<tree>/<patient_id>_surveys_and_settings.json """
        filename = self.participant.study.object_id + "_surveys_and_settings.json"
        return path_join(self.tree_base_path, filename)
    
    @property
    def all_bv_set_path(self) -> str:
        """ Jasmine's all_bv_set file for this task.
        /tmp/forest/<uuid>/<tree>/output/all_BV_set.pkl """
        return path_join(self.data_output_path, "all_BV_set.pkl")
    
    @property
    def all_memory_dict_path(self) -> str:
        """ Jasmine's all_memory_dict file for this task. 
        /tmp/forest/<uuid>/<tree>/output/all_memory_dict.pkl """
        return path_join(self.data_output_path, "all_memory_dict.pkl")
    
    #
    ## AWS S3 key paths
    #
    @property
    def s3_base_folder(self) -> str:
        """ Base file path on AWS S3 for any forest data on this study. """
        return path_join(self.participant.study.object_id, "forest")
    
    @property
    def all_bv_set_s3_key_path(self):
        """ Jasmine's all_bv_set file for this study on AWS S3 - applies to all participants. """
        return path_join(self.s3_base_folder, 'all_bv_set.pkl')
    
    @property
    def all_memory_dict_s3_key_path(self):
        """ Jasmine's all_memory_dict file for this study on AWS S3 - applies to all participants. """
        return path_join(self.s3_base_folder, 'all_memory_dict.pkl')


class SummaryStatisticDaily(TimestampedModel):
    participant: Participant = models.ForeignKey(Participant, on_delete=models.CASCADE)
    date = models.DateField(db_index=True)
    timezone = models.CharField(max_length=10, null=False, blank=False)  # abbreviated time zone names are max 4 chars.
    
    # Beiwe data quantities
    beiwe_accelerometer_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_ambient_audio_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_app_log_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_bluetooth_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_omniring_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_calls_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_devicemotion_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_gps_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_gyro_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_identifiers_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_ios_log_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_magnetometer_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_power_state_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_proximity_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_reachability_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_survey_answers_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_survey_timings_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_texts_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_audio_recordings_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    beiwe_wifi_bytes = models.PositiveBigIntegerField(null=True, blank=True)
    
    # GPS
    jasmine_distance_diameter = models.FloatField(null=True, blank=True)
    jasmine_distance_from_home = models.FloatField(null=True, blank=True)
    jasmine_distance_traveled = models.FloatField(null=True, blank=True)
    jasmine_flight_distance_average = models.FloatField(null=True, blank=True)
    jasmine_flight_distance_stddev = models.FloatField(null=True, blank=True)
    jasmine_flight_duration_average = models.FloatField(null=True, blank=True)
    jasmine_flight_duration_stddev = models.FloatField(null=True, blank=True)
    jasmine_gps_data_missing_duration = models.IntegerField(null=True, blank=True)
    jasmine_home_duration = models.FloatField(null=True, blank=True)
    jasmine_gyration_radius = models.FloatField(null=True, blank=True)
    jasmine_significant_location_count = models.IntegerField(null=True, blank=True)
    jasmine_significant_location_entropy = models.FloatField(null=True, blank=True)
    jasmine_pause_time = models.TextField(null=True, blank=True)
    jasmine_obs_duration = models.FloatField(null=True, blank=True)
    jasmine_obs_day = models.FloatField(null=True, blank=True)
    jasmine_obs_night = models.FloatField(null=True, blank=True)
    jasmine_total_flight_time = models.FloatField(null=True, blank=True)
    jasmine_av_pause_duration = models.FloatField(null=True, blank=True)
    jasmine_sd_pause_duration = models.FloatField(null=True, blank=True)
    
    # Willow, Texts
    willow_incoming_text_count = models.IntegerField(null=True, blank=True)
    willow_incoming_text_degree = models.IntegerField(null=True, blank=True)
    willow_incoming_text_length = models.IntegerField(null=True, blank=True)
    willow_outgoing_text_count = models.IntegerField(null=True, blank=True)
    willow_outgoing_text_degree = models.IntegerField(null=True, blank=True)
    willow_outgoing_text_length = models.IntegerField(null=True, blank=True)
    willow_incoming_text_reciprocity = models.IntegerField(null=True, blank=True)
    willow_outgoing_text_reciprocity = models.IntegerField(null=True, blank=True)
    willow_outgoing_MMS_count = models.IntegerField(null=True, blank=True)
    willow_incoming_MMS_count = models.IntegerField(null=True, blank=True)
    
    # Willow, Calls
    willow_incoming_call_count = models.IntegerField(null=True, blank=True)
    willow_incoming_call_degree = models.IntegerField(null=True, blank=True)
    willow_incoming_call_duration = models.FloatField(null=True, blank=True)
    willow_outgoing_call_count = models.IntegerField(null=True, blank=True)
    willow_outgoing_call_degree = models.IntegerField(null=True, blank=True)
    willow_outgoing_call_duration = models.FloatField(null=True, blank=True)
    willow_missed_call_count = models.IntegerField(null=True, blank=True)
    willow_missed_callers = models.IntegerField(null=True, blank=True)
    
    willow_uniq_individual_call_or_text_count = models.IntegerField(null=True, blank=True)
    
    # Sycamore, Survey Frequency
    sycamore_total_surveys = models.IntegerField(null=True, blank=True)
    sycamore_total_completed_surveys = models.IntegerField(null=True, blank=True)
    sycamore_total_opened_surveys = models.IntegerField(null=True, blank=True)
    sycamore_average_time_to_submit = models.FloatField(null=True, blank=True)
    sycamore_average_time_to_open = models.FloatField(null=True, blank=True)
    sycamore_average_duration = models.FloatField(null=True, blank=True)
    
    # Oak, walking statistics
    oak_walking_time = models.FloatField(null=True, blank=True)
    oak_steps = models.FloatField(null=True, blank=True)
    oak_cadence = models.FloatField(null=True, blank=True)
    
    # points to the task that populated this data set. ()
    jasmine_task: ForestTask = models.ForeignKey(ForestTask, blank=True, null=True, on_delete=models.PROTECT,
                                                 related_name="jasmine_summary_statistics")
    willow_task: ForestTask = models.ForeignKey(ForestTask, blank=True, null=True, on_delete=models.PROTECT,
                                                related_name="willow_summary_statistics")
    sycamore_task: ForestTask = models.ForeignKey(ForestTask, blank=True, null=True, on_delete=models.PROTECT,
                                                  related_name="sycamore_summary_statistics")
    oak_task: ForestTask = models.ForeignKey(ForestTask, blank=True, null=True, on_delete=models.PROTECT,
                                             related_name="oak_summary_statistics")
    
    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['date', 'participant'], name="unique_summary_statistic")
        ]
    
    @classmethod
    def beiwe_fields(cls):
        return [field.name for field in cls._meta.get_fields() if field.name.startswith("beiwe_")]
    
    @classmethod
    def jasmine_fields(cls):
        return [field.name for field in cls._meta.get_fields() if field.name.startswith("jasmine_")]
    
    @classmethod
    def willow_fields(cls):
        return [field.name for field in cls._meta.get_fields() if field.name.startswith("willow_")]
    
    @classmethod
    def sycamore_fields(cls):
        return [field.name for field in cls._meta.get_fields() if field.name.startswith("sycamore_")]
    
    @classmethod
    def oak_fields(cls):
        return [field.name for field in cls._meta.get_fields() if field.name.startswith("oak_")]
