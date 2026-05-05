from __future__ import annotations

import pickle
import uuid
from datetime import timedelta
from os.path import join as path_join

from django.db.models import (AutoField, BigIntegerField, BinaryField, BooleanField, CASCADE,
    CharField, DateField, DateTimeField, FloatField, ForeignKey, IntegerField, Manager,
    PositiveBigIntegerField, PROTECT, TextField, UniqueConstraint, UUIDField)

from config.settings import DOMAIN_NAME
from constants.celery_constants import ForestTaskStatus
from constants.forest_constants import (DEFAULT_FOREST_PARAMETERS, FOREST_PICKLING_ERROR,
    ForestTree, NON_PICKLED_PARAMETERS, OAK_DATE_FORMAT_PARAMETER, ROOT_FOREST_TASK_PATH, SYCAMORE_DATE_FORMAT)
from database.models import Participant, Study, TimestampedModel
from libs.utils.date_utils import datetime_to_list
from libs.utils.forest_utils import (assemble_jasmine_dynamic_params,
    assemble_sycamore_params)


#
## GO READ THE MULTILINE STATEMENT AT THE TOP OF services/celery_forest.py
#


class ForestTask(TimestampedModel):
    # All forest tasks are defined to be associated with a single participant
    # The 'the_study' field is name with the definite article for some weird compatibility reason...
    the_study: Study = ForeignKey(Study, on_delete=PROTECT, related_name="forest_tasks")
    participant: Participant | None = ForeignKey(Participant, on_delete=PROTECT, db_index=True, null=True)
    
    forest_tree: str = TextField(choices=ForestTree.choices())
    forest_version = CharField(blank=True, max_length=10, null=False, default="")
    forest_commit = CharField(blank=True, max_length=40, null=False, default="")
    
    # the external id is used for endpoints that refer to forest trackers to avoid exposing the
    # primary keys of the model. it is intentionally not the primary key
    external_id = UUIDField(default=uuid.uuid4, editable=False)
    
    # due to code churn we pickle parameters that are passed to forest.
    pickled_parameters = BinaryField(blank=True, null=True)
    # all forest tasks run on a data range, (these are parameters entered at creation from the web)
    data_date_start = DateField()  # inclusive
    data_date_end = DateField()  # inclusive
    
    # runtime records
    total_file_size = BigIntegerField(blank=True, null=True)  # input file size sum for accounting
    process_start_time = DateTimeField(null=True, blank=True)
    process_download_end_time = DateTimeField(null=True, blank=True)
    process_end_time = DateTimeField(null=True, blank=True)
    status = TextField(choices=ForestTaskStatus.choices())
    stacktrace = TextField(null=True, blank=True, default=None)
    # Whether or not there was any data output by Forest (None means construct_summary_statistics errored)
    forest_output_exists = BooleanField(null=True, blank=True)
    
    # S3 file paths
    output_zip_s3_path = TextField(blank=True)  # includes study folder in path
    # Jasmine has special parameters, these are their s3 file paths.
    all_bv_set_s3_key = TextField(blank=True)
    all_memory_dict_s3_key = TextField(blank=True)
    
    # related field typings (IDE halp)
    jasmine_summary_statistics: Manager[SummaryStatisticDaily]
    willow_summary_statistics: Manager[SummaryStatisticDaily]
    oak_summary_statistics: Manager[SummaryStatisticDaily]
    
    sycamore_analysis_output: Manager[SycamoreAnalysisOutput]
    
    @property
    def taskname(self) -> str:
        # this is the Foreign key reference field name in SummaryStatisticDaily
        return self.forest_tree + "_task"
    
    @property
    def sentry_tags(self) -> dict[str, str|uuid.UUID|bool|None]:
        from libs.utils.http_utils import easy_url
        url = path_join(DOMAIN_NAME, easy_url("forest_endpoints.task_log", study_id=self.the_study.id))
        return {
            "participant": self.participant.patient_id if self.participant else "None",
            "study": self.the_study.name,
            "study_objectid": self.the_study.object_id,
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
            "forest_output_exists": self.forest_output_exists,
            "output_zip_s3_path": self.output_zip_s3_path,
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
            "output_folder": self.data_output_folder_path,
            "study_folder": self.data_input_path,
            "tz_str": self.the_study.timezone_name,
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
    
    def pickle_to_pickled_parameters(self, parameters: dict):
        """ takes parameters and pickles them """
        if not isinstance(parameters, dict):
            raise TypeError("parameters must be a dict")
        # we need to clear but certain parameters, but we don't want to mutate the dictionary
        cleaned_parameters = parameters.copy()
        for parameter in NON_PICKLED_PARAMETERS:
            cleaned_parameters.pop(parameter, None)
        self.pickled_parameters = pickle.dumps(cleaned_parameters)
        self.save()
    
    def unpickle_from_pickled_parameters(self) -> dict:
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
    
    def handle_tree_specific_params(self, params: dict):
        self.handle_tree_specific_date_params(params)
        if self.forest_tree == ForestTree.jasmine:
            assemble_jasmine_dynamic_params(self, params)
        if self.forest_tree == ForestTree.sycamore:
            assemble_sycamore_params(self, params)
    
    # TODO: forest uses date components/strings because previously we did not pickle the parameters.
    def handle_tree_specific_date_params(self, params: dict):
        
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
    
    #
    ## File paths
    #
    
    ## Base paths everything else is based on
    
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
    
    ## Output paths
    
    @property
    def data_output_folder_path(self) -> str:
        """ Path to the output data folder. /tmp/forest/<uuid>/<tree>/output """
        return path_join(self.tree_base_path, "output")
    
    @property
    def task_report_path(self) -> str:
        """ Path to the task report file. /tmp/forest/<uuid>/<tree>/output/task_report.txt """
        return path_join(self.data_output_folder_path, "task_report.txt")
    
    @property
    def summary_statistics_results_path(self) -> str:
        """ Path to the file that contains the output of Forest. """
        # /tmp/forest/<uuid>/<tree>/output/daily/<patient_id>.csv
        return path_join(self.data_output_folder_path, "daily", f"{self.participant.patient_id}.csv")
    
    @property
    def sycamore_output_file(self) -> str:
        """ Path to the file that contains the output of Sycamore. """
        # /tmp/forest/<uuid>/<tree>/output/sycamore_output.csv
        return path_join(self.data_output_folder_path, SycamoreAnalysisOutput.SOURCE_DATA_FILE_PATH)
    
    ## Input paths
    
    @property
    def input_interventions_file(self) -> str:
        """ The study interventions file path for the participant's survey data. """
        # /tmp/forest/<uuid>/<tree>/<study_objectid>_interventions.json
        filename = self.the_study.object_id + "_interventions.json"
        return path_join(self.tree_base_path, filename)
    
    @property
    def input_study_config_file(self) -> str:
        """ The study configuration file file path. """
        # /tmp/forest/<uuid>/<tree>/<study_objectid>_surveys_and_settings.json
        filename = self.the_study.object_id + "_surveys_and_settings.json"
        return path_join(self.tree_base_path, filename)
    
    @property
    def input_survey_history_file(self) -> str:
        """ The survey history file for the study. """
        # /tmp/forest/<uuid>/<tree>/<study_objectid>_survey_history.json
        filename = self.the_study.object_id + "_survey_history.json"
        return path_join(self.tree_base_path, filename)
    
    ## Obscure uploaded asset paths (output)
    
    @property
    def all_bv_set_path(self) -> str:
        """ Jasmine's all_bv_set file for this task. """
        # /tmp/forest/<uuid>/<tree>/output/all_BV_set.pkl
        return path_join(self.data_output_folder_path, "all_BV_set.pkl")
    
    @property
    def all_memory_dict_path(self) -> str:
        """ Jasmine's all_memory_dict file for this task. """
        # /tmp/forest/<uuid>/<tree>/output/all_memory_dict.pkl
        return path_join(self.data_output_folder_path, "all_memory_dict.pkl")
    
    #
    ## AWS S3 key paths
    #
    
    @property
    def s3_base_folder(self) -> str:
        """ Base file path on AWS S3 for any forest data on this study. """
        return path_join(self.the_study.object_id, "forest")
    
    @property
    def all_bv_set_s3_key_path(self):
        """ Jasmine's all_bv_set file for this study on AWS S3 - applies to all participants. """
        return path_join(self.s3_base_folder, 'all_bv_set.pkl')
    
    @property
    def all_memory_dict_s3_key_path(self):
        """ Jasmine's all_memory_dict file for this study on AWS S3 - applies to all participants. """
        return path_join(self.s3_base_folder, 'all_memory_dict.pkl')


class SummaryStatisticDaily(TimestampedModel):
    the_study: Study = ForeignKey(Study, on_delete=PROTECT, related_name="summary_statistics_daily")
    participant: Participant = ForeignKey(Participant, on_delete=CASCADE)
    date = DateField(db_index=True)
    timezone = CharField(max_length=10, null=False, blank=False)  # abbreviated time zone names are max 4 chars.
    
    @classmethod
    def default_summary_statistic_daily_cheatsheet(cls) -> dict:
        
        # this is used to populate default values in a SummaryStatisticDaily in a way that creates
        # legible output when something goes wrong in a test.
        field_dict = {}
        for i, field in enumerate(cls._meta.fields):
            i=i-1  # we added "the_study" field and it changes tests... just offsetting here instead.
            if isinstance(field, (ForeignKey, DateField, AutoField)):
                continue
            
            elif isinstance(field, IntegerField):
                field_dict[field.name] = i
            elif isinstance(field, FloatField):
                field_dict[field.name] = float(i)
            elif isinstance(field, (TextField, CharField)):
                field_dict[field.name] = str(i)
            else:
                raise TypeError(f"encountered unhandled SummaryStatisticDaily type: {type(field)}")
        return field_dict
    
    # Beiwe data quantities
    beiwe_accelerometer_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_ambient_audio_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_app_log_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_bluetooth_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_calls_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_devicemotion_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_gps_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_gyro_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_identifiers_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_ios_log_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_magnetometer_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_power_state_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_proximity_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_reachability_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_survey_answers_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_survey_timings_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_texts_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_audio_recordings_bytes = PositiveBigIntegerField(null=True, blank=True)
    beiwe_wifi_bytes = PositiveBigIntegerField(null=True, blank=True)
    
    # GPS
    jasmine_distance_diameter = FloatField(null=True, blank=True)
    jasmine_distance_from_home = FloatField(null=True, blank=True)
    jasmine_distance_traveled = FloatField(null=True, blank=True)
    jasmine_flight_distance_average = FloatField(null=True, blank=True)
    jasmine_flight_distance_stddev = FloatField(null=True, blank=True)
    jasmine_flight_duration_average = FloatField(null=True, blank=True)
    jasmine_flight_duration_stddev = FloatField(null=True, blank=True)
    jasmine_home_duration = FloatField(null=True, blank=True)
    jasmine_gyration_radius = FloatField(null=True, blank=True)
    jasmine_significant_location_count = IntegerField(null=True, blank=True)
    jasmine_significant_location_entropy = FloatField(null=True, blank=True)
    jasmine_pause_time = TextField(null=True, blank=True)
    jasmine_obs_duration = FloatField(null=True, blank=True)
    jasmine_obs_day = FloatField(null=True, blank=True)
    jasmine_obs_night = FloatField(null=True, blank=True)
    jasmine_total_flight_time = FloatField(null=True, blank=True)
    jasmine_av_pause_duration = FloatField(null=True, blank=True)
    jasmine_sd_pause_duration = FloatField(null=True, blank=True)
    
    # Willow, Texts
    willow_incoming_text_count = IntegerField(null=True, blank=True)
    willow_incoming_text_degree = IntegerField(null=True, blank=True)
    willow_incoming_text_length = IntegerField(null=True, blank=True)
    willow_outgoing_text_count = IntegerField(null=True, blank=True)
    willow_outgoing_text_degree = IntegerField(null=True, blank=True)
    willow_outgoing_text_length = IntegerField(null=True, blank=True)
    willow_incoming_text_reciprocity = IntegerField(null=True, blank=True)
    willow_outgoing_text_reciprocity = IntegerField(null=True, blank=True)
    willow_outgoing_MMS_count = IntegerField(null=True, blank=True)
    willow_incoming_MMS_count = IntegerField(null=True, blank=True)
    
    # Willow, Calls
    willow_incoming_call_count = IntegerField(null=True, blank=True)
    willow_incoming_call_degree = IntegerField(null=True, blank=True)
    willow_incoming_call_duration = FloatField(null=True, blank=True)
    willow_outgoing_call_count = IntegerField(null=True, blank=True)
    willow_outgoing_call_degree = IntegerField(null=True, blank=True)
    willow_outgoing_call_duration = FloatField(null=True, blank=True)
    willow_missed_call_count = IntegerField(null=True, blank=True)
    willow_missed_callers = IntegerField(null=True, blank=True)
    willow_mean_responsiveness_call = FloatField(null=True, blank=True)
    willow_call_reciprocity = FloatField(null=True, blank=True)
    
    willow_uniq_individual_call_or_text_count = IntegerField(null=True, blank=True)
    
    # Oak, walking statistics
    oak_walking_time = FloatField(null=True, blank=True)
    oak_steps = FloatField(null=True, blank=True)
    oak_cadence = FloatField(null=True, blank=True)
    
    # points to the task that populated this data set. ()
    jasmine_task: ForestTask = ForeignKey(ForestTask, blank=True, null=True, on_delete=PROTECT, related_name="jasmine_summary_statistics")
    willow_task: ForestTask = ForeignKey(ForestTask, blank=True, null=True, on_delete=PROTECT, related_name="willow_summary_statistics")
    oak_task: ForestTask = ForeignKey(ForestTask, blank=True, null=True, on_delete=PROTECT, related_name="oak_summary_statistics")
    
    class Meta:
        constraints = [
            UniqueConstraint(fields=['date', 'participant'], name="unique_summary_statistic")
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
    def oak_fields(cls):
        return [field.name for field in cls._meta.get_fields() if field.name.startswith("oak_")]


# contains the output of data from runs of the Onnela Lab Forest Tree called Sycamore
class SycamoreAnalysisOutput(TimestampedModel):
    
    SOURCE_DATA_FILE_PATH = "sycamore_output.csv"
    
    forest_task: ForestTask = ForeignKey(ForestTask, blank=True, null=True, on_delete=PROTECT, related_name="sycamore_analysis_output")
    study: Study = ForeignKey(Study, on_delete=PROTECT, related_name="sycamore_analyses")
    
    obs_duration = FloatField(null=True)
    obs_day = FloatField(null=True)
    obs_night = FloatField(null=True)
    home_duration = FloatField(null=True)
    distance_traveled = FloatField(null=True)
    distance_from_home = FloatField(null=True)
    gyration_radius = FloatField(null=True)
    distance_diameter = FloatField(null=True)
    significant_location_count = FloatField(null=True)
    significant_location_entropy = FloatField(null=True)
    total_flight_time = FloatField(null=True)
    flight_distance_average = FloatField(null=True)
    flight_distance_stddev = FloatField(null=True)
    flight_duration_average = FloatField(null=True)
    flight_duration_stddev = FloatField(null=True)
    pause_time = FloatField(null=True)
    av_pause_duration = FloatField(null=True)
    sd_pause_duration = FloatField(null=True)
    physical_circadian_rhythm = FloatField(null=True)
    physical_circadian_rhythm_stratified = FloatField(null=True)
