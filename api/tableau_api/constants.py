from django.db.models.fields import IntegerField, FloatField, DateTimeField, DateField, BooleanField

SERIALIZABLE_FIELD_NAMES = [
    # Metadata
    "date",
    "participant_id",
    "study_id",
    
    # GPS
    "distance_diameter",
    "distance_from_home",
    "distance_traveled",
    "flight_distance_average",
    "flight_distance_standard_deviation",
    "flight_duration_average",
    "flight_duration_standard_deviation",
    "gps_data_missing_duration",
    "home_duration",
    # "physical_circadian_rhythm",
    # "physical_circadian_rhythm_stratified",
    # "radius_of_gyration",
    # "significant_location_count",
    # "significant_location_entropy",
    # "stationary_fraction",
    
    # Texts
    # "text_incoming_count",
    # "text_incoming_degree",
    # "text_incoming_length",
    # "text_incoming_responsiveness",
    # "text_outgoing_count",
    # "text_outgoing_degree",
    # "text_outgoing_length",
    # "text_reciprocity",
    
    # Calls
    # "call_incoming_count",
    # "call_incoming_degree",
    # "call_incoming_duration",
    # "call_incoming_responsiveness",
    # "call_outgoing_count",
    # "call_outgoing_degree",
    # "call_outgoing_duration",
    
    # Accelerometer
    # "acceleration_direction",
    # "accelerometer_coverage_fraction",
    # "accelerometer_signal_variability",
    # "accelerometer_univariate_summaries",
    # "device_proximity",
    
    # Power state
    # "total_power_events",
    # "total_screen_events",
    # "total_unlock_events",
    
    # Multiple domains
    # "awake_onset_time",
    # "sleep_duration",
    # "sleep_onset_time",
]

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
]


X_ACCESS_KEY_ID = "X-Access-Key-Id"
X_ACCESS_KEY_SECRET = "X-Access-Key-Secret"

# general error messages
CREDENTIALS_NOT_VALID_ERROR_MESSAGE = "Credentials not valid"
HEADER_IS_REQUIRED = "This header is required"
RESOURCE_NOT_FOUND = "resource not found"

# permissions errors
APIKEY_NO_ACCESS_MESSAGE = "ApiKey does not have access to Tableau API"
NO_STUDY_PROVIDED_MESSAGE = "No study id specified"
NO_STUDY_FOUND_MESSAGE = "No matching study found"
RESEARCHER_NOT_ALLOWED = "Researcher does not have permission to view that study"
STUDY_HAS_FOREST_DISABLED_MESSAGE = "Study does not have forest enabled"
