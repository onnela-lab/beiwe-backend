from django.db.models.fields import (BooleanField, CharField, DateField, DateTimeField, FloatField,
    IntegerField, TextField)

from constants.forest_constants import ForestTree, TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS


# Metadata
SUMMARY_METADATA_FIELD_NAMES = [
    "date",
    "participant_id",
    "study_id",
    "timezone",
]

NICE_SUMMARY_METADATA_FIELD_NAMES = [
    name.replace("_", " ").title() for name in SUMMARY_METADATA_FIELD_NAMES
]

# Data quantities
DATA_QUANTITY_FIELD_NAMES = [
    "beiwe_accelerometer_bytes",
    "beiwe_ambient_audio_bytes",
    "beiwe_app_log_bytes",
    "beiwe_bluetooth_bytes",
    "beiwe_calls_bytes",
    "beiwe_devicemotion_bytes",
    "beiwe_gps_bytes",
    "beiwe_gyro_bytes",
    "beiwe_identifiers_bytes",
    "beiwe_ios_log_bytes",
    "beiwe_magnetometer_bytes",
    "beiwe_power_state_bytes",
    "beiwe_proximity_bytes",
    "beiwe_reachability_bytes",
    "beiwe_survey_answers_bytes",
    "beiwe_survey_timings_bytes",
    "beiwe_texts_bytes",
    "beiwe_audio_recordings_bytes",
    "beiwe_wifi_bytes",
]

NICE_BEIWE_DATA_QUANTITY_FIELD_NAMES = [
    name.replace("beiwe_", "").replace("_", " ").title() for name in DATA_QUANTITY_FIELD_NAMES
]

JASMINE_FIELDS = [
    # GPS
    "jasmine_distance_diameter",
    "jasmine_distance_from_home",
    "jasmine_distance_traveled",
    "jasmine_flight_distance_average",
    "jasmine_flight_distance_stddev",
    "jasmine_flight_duration_average",
    "jasmine_flight_duration_stddev",
    "jasmine_gps_data_missing_duration",
    "jasmine_home_duration",
    "jasmine_gyration_radius",
    "jasmine_significant_location_count",
    "jasmine_significant_location_entropy",
    "jasmine_pause_time",
    "jasmine_obs_duration",
    "jasmine_obs_day",
    "jasmine_obs_night",
    "jasmine_total_flight_time",
    "jasmine_av_pause_duration",
    "jasmine_sd_pause_duration",
]
NICE_JASMINE_FIELDS = [
    name.replace("jasmine_", "").replace("_", " ").title() for name in JASMINE_FIELDS
]


WILLOW_FIELDS = [
    # Willow, Texts
    "willow_incoming_text_count",
    "willow_incoming_text_degree",
    "willow_incoming_text_length",
    "willow_outgoing_text_count",
    "willow_outgoing_text_degree",
    "willow_outgoing_text_length",
    "willow_incoming_text_reciprocity",
    "willow_outgoing_text_reciprocity",
    "willow_outgoing_MMS_count",
    "willow_incoming_MMS_count",
    # Willow, Calls
    "willow_incoming_call_count",
    "willow_incoming_call_degree",
    "willow_incoming_call_duration",
    "willow_outgoing_call_count",
    "willow_outgoing_call_degree",
    "willow_outgoing_call_duration",
    "willow_missed_call_count",
    "willow_missed_callers",
    
    "willow_uniq_individual_call_or_text_count",
]

NICE_WILLOW_FIELDS = [
    name.replace("willow_", "").replace("_", " ").title() for name in WILLOW_FIELDS
]

SYCAMORE_FIELDS = [
    # Sycamore, Survey Frequency
    "sycamore_total_surveys",
    "sycamore_total_completed_surveys",
    "sycamore_total_opened_surveys",
    "sycamore_average_time_to_submit",
    "sycamore_average_time_to_open",
    "sycamore_average_duration",
]

NICE_SYCAMORE_FIELDS = [
    name.replace("sycamore_", "").replace("_", " ").title() for name in SYCAMORE_FIELDS
]

OAK_FIELDS = [
    # Oak, walking statistics
    "oak_walking_time",
    "oak_steps",
    "oak_cadence",
]

NICE_OAK_FIELDS = [
    name.replace("oak_", "").replace("_", " ").title() for name in OAK_FIELDS
]

SERIALIZABLE_FIELD_NAMES = SUMMARY_METADATA_FIELD_NAMES + DATA_QUANTITY_FIELD_NAMES \
    + JASMINE_FIELDS + WILLOW_FIELDS + SYCAMORE_FIELDS + OAK_FIELDS

# SERIALIZABLE_FIELD_NAMES.extend(TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS.values())
NICE_SERIALIZABLE_FIELD_NAMES = NICE_SUMMARY_METADATA_FIELD_NAMES + NICE_BEIWE_DATA_QUANTITY_FIELD_NAMES \
    + NICE_JASMINE_FIELDS + NICE_WILLOW_FIELDS + NICE_SYCAMORE_FIELDS + NICE_OAK_FIELDS


FOREST_TREE_TO_SERIALIZABLE_FIELD_NAMES = {
    ForestTree.jasmine: JASMINE_FIELDS,
    ForestTree.willow: WILLOW_FIELDS,
    ForestTree.sycamore: SYCAMORE_FIELDS,
    ForestTree.oak: OAK_FIELDS,
}


SERIALIZABLE_FIELD_NAMES_DROPDOWN = [(f, f) for f in SERIALIZABLE_FIELD_NAMES]

VALID_QUERY_PARAMETERS = [
    "end_date",
    "fields",
    "limit",
    "order_direction",
    "ordered_by",
    "participant_ids",
    "start_date",
    "study_id",
]

# maps django fields to tableau data types. All fields not included here are interpreted as string data in tableau
# note that this process considers subclasses, so all subclasses of DateFields will appear in tableau as a data
FIELD_TYPE_MAP = [
    (IntegerField, 'tableau.dataTypeEnum.int'),
    (FloatField, 'tableau.dataTypeEnum.float'),
    (DateTimeField, 'tableau.dataTypeEnum.datetime'),
    (DateField, 'tableau.dataTypeEnum.date'),
    (BooleanField, 'tableau.dataTypeEnum.bool'),
    (CharField, 'tableau.dataTypeEnum.string'),
    (TextField, 'tableau.dataTypeEnum.string'),
]


X_ACCESS_KEY_ID = "X-Access-Key-Id"
X_ACCESS_KEY_SECRET = "X-Access-Key-Secret"

# general error messages
CREDENTIALS_NOT_VALID_ERROR_MESSAGE = "Credentials not valid"
HEADER_IS_REQUIRED = "This header is required"
RESOURCE_NOT_FOUND = "resource not found"

# permissions errors
NO_STUDY_PROVIDED_MESSAGE = "No study id specified"
NO_STUDY_FOUND_MESSAGE = "No matching study found"
RESEARCHER_NOT_ALLOWED = "Researcher does not have permission to view that study"
STUDY_HAS_FOREST_DISABLED_MESSAGE = "Study does not have forest enabled"
