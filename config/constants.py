# from os import getenv
from os.path import join as path_join

from config.settings import DOMAIN_NAME

# This string will be printed into non-error hourly reports to improve error filtering.
DATA_PROCESSING_NO_ERROR_STRING = "2HEnBwlawY"

## Data streams and survey types ##
ALLOWED_EXTENSIONS = {'csv', 'json', 'mp4', "wav", 'txt', 'jpg'}
PROCESSABLE_FILE_EXTENSIONS = [".csv", ".mp4", ".wav"]
# These don't appear to be used...
MEDIA_EXTENSIONS = [".mp4", ".wav", ".jpg"]

## HTML lists ##
CHECKBOX_TOGGLES = [
    "accelerometer",
    "gps",
    "calls",
    "texts",
    "wifi",
    "bluetooth",
    "power_state",
    "proximity",
    "gyro",
    "magnetometer",
    "devicemotion",
    "ambient_audio",
    "reachability",
    "allow_upload_over_cellular_data",
    "use_anonymized_hashing",
    "use_gps_fuzzing",
    "call_clinician_button_enabled",
    "call_research_assistant_button_enabled"
]

TIMER_VALUES = [
    "accelerometer_off_duration_seconds",
    "accelerometer_on_duration_seconds",
    "bluetooth_on_duration_seconds",
    "bluetooth_total_duration_seconds",
    "bluetooth_global_offset_seconds",
    "check_for_new_surveys_frequency_seconds",
    "create_new_data_files_frequency_seconds",
    "gps_off_duration_seconds",
    "gps_on_duration_seconds",
    "seconds_before_auto_logout",
    "upload_data_files_frequency_seconds",
    "voice_recording_max_time_length_seconds",
    "wifi_log_frequency_seconds",
    "gyro_off_duration_seconds",
    "gyro_on_duration_seconds",
    "magnetometer_off_duration_seconds",
    "magnetometer_on_duration_seconds",
    "devicemotion_off_duration_seconds",
    "devicemotion_on_duration_seconds"
]

# The format that dates should be in throughout the codebase
# 1990-01-31T07:30:04 gets you jan 31 1990 at 7:30:04am
# human string is YYYY-MM-DDThh:mm:ss
API_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
API_TIME_FORMAT_WITH_TZ = "%Y-%m-%dT%H:%M:%S (%Z)"
API_DATE_FORMAT = "%Y-%m-%d"
DEV_TIME_FORMAT = "%Y-%m-%d %H:%M (%Z)"

## Chunks
# This value is in seconds, it sets the time period that chunked files will be sliced into.
CHUNK_TIMESLICE_QUANTUM = 3600
# the name of the s3 folder that contains chunked data
CHUNKS_FOLDER = "CHUNKED_DATA"

## Constants for for the keys in DATA_STREAM_TO_S3_FILE_NAME_STRING
ACCELEROMETER = "accelerometer"
AMBIENT_AUDIO = "ambient_audio"
ANDROID_LOG_FILE = "app_log"
BLUETOOTH = "bluetooth"
CALL_LOG = "calls"
DEVICEMOTION = "devicemotion"
GPS = "gps"
GYRO = "gyro"
IDENTIFIERS = "identifiers"
IMAGE_FILE = "image_survey"
IOS_LOG_FILE = "ios_log"
MAGNETOMETER = "magnetometer"
POWER_STATE = "power_state"
PROXIMITY = "proximity"
REACHABILITY = "reachability"
SURVEY_ANSWERS = "survey_answers"
SURVEY_TIMINGS = "survey_timings"
TEXTS_LOG = "texts"
VOICE_RECORDING = "audio_recordings"
WIFI = "wifi"

# dictionary for printing PROCESSED data streams for frontend
# necessary even with the complete data stream dict for parsing data from the backend
PROCESSED_DATA_STREAM_DICT = {
    "responsiveness": "Responsiveness",
    "outgoing_calllengths": "Outgoing Call Lengths",
    "call_indegree": "Call In Degree",
    "incoming_calllengths": "Incoming Call Lengths",
    "reciprocity": "Reciprocity",
    "call_outdegree": "Call Out Degree",
    "incoming_calls": "Incoming Calls",
    "outgoing_calls": "Outgoing Calls",
    "outgoing_textlengths": "Outgoing Text Lengths",
    "text_indegree": "Text In Degree",
    "incoming_textlengths": "Incoming Text Lengths",
    "text_outdegree": "Text Out Degree",
    "incoming_texts": "Incoming Texts",
    "outgoing_texts": "Outgoing Texts",
    "RoG_km": "RoG (km)",
    "MaxDiam_km": "Maximum Diameter (km)",
    "StdFlightDur_min": "Standard Flight Duration (min)",
    "AvgFlightLen_km": "Average Flight Length (km)",
    "Hometime_hrs": "Home Time (hours)",
    "AvgFlightDur_min": "Average Flight Duration (min)",
    "DistTravelled_km": "Distance Travelled (km)",
    "StdFlightLen_km": "Standard Flight Length (km)",
    "MaxHomeDist_km": "Maximum Home Distance (km)",
}

# dictionary for printing ALL data streams (processed and bytes)
COMPLETE_DATA_STREAM_DICT = {
    "responsiveness": "Responsiveness",
    "outgoing_calllengths": "Outgoing Call Lengths",
    "call_indegree": "Call In Degree",
    "incoming_calllengths": "Incoming Call Lengths",
    "reciprocity": "Reciprocity",
    "call_outdegree": "Call Out Degree",
    "incoming_calls": "Incoming Calls",
    "outgoing_calls": "Outgoing Calls",
    "outgoing_textlengths": "Outgoing Text Lengths",
    "text_indegree": "Text In Degree",
    "incoming_textlengths": "Incoming Text Lengths",
    "text_outdegree": "Text Out Degree",
    "incoming_texts": "Incoming Texts",
    "outgoing_texts": "Outgoing Texts",
    "RoG_km": "RoG (km)",
    "MaxDiam_km": "Maximum Diameter (km)",
    "StdFlightDur_min": "Standard Flight Duration (min)",
    "AvgFlightLen_km": "Average Flight Length (km)",
    "Hometime_hrs": "Home Time (hours)",
    "AvgFlightDur_min": "Average Flight Duration (min)",
    "DistTravelled_km": "Distance Travelled (km)",
    "StdFlightLen_km": "Standard Flight Length (km)",
    "MaxHomeDist_km": "Maximum Home Distance (km)",
    ACCELEROMETER: "Accelerometer (bytes)",
    AMBIENT_AUDIO: "Ambient Audio Recording (bytes)",
    ANDROID_LOG_FILE: "Android Log File (bytes)",
    BLUETOOTH: "Bluetooth (bytes)",
    CALL_LOG: "Call Log (bytes)",
    DEVICEMOTION: "Device Motion (bytes)",
    GPS: "GPS (bytes)",
    GYRO: "Gyro (bytes)",
    IDENTIFIERS: "Identifiers (bytes)",
    IMAGE_FILE: "Image Survey (bytes)",
    IOS_LOG_FILE: "iOS Log File (bytes)",
    MAGNETOMETER: "Magnetometer (bytes)",
    POWER_STATE: "Power State (bytes)",
    PROXIMITY: "Proximity (bytes)",
    REACHABILITY: "Reachability (bytes)",
    SURVEY_ANSWERS: "Survey Answers (bytes)",
    SURVEY_TIMINGS: "Survey Timings (bytes)",
    TEXTS_LOG: "Text Log (bytes)",
    VOICE_RECORDING: "Audio Recordings (bytes)",
    WIFI: "Wifi (bytes)",
}

ALL_DATA_STREAMS = [
    ACCELEROMETER,
    AMBIENT_AUDIO,
    ANDROID_LOG_FILE,
    BLUETOOTH,
    CALL_LOG,
    DEVICEMOTION,
    GPS,
    GYRO,
    IDENTIFIERS,
    IMAGE_FILE,
    IOS_LOG_FILE,
    MAGNETOMETER,
    POWER_STATE,
    PROXIMITY,
    REACHABILITY,
    SURVEY_ANSWERS,
    SURVEY_TIMINGS,
    TEXTS_LOG,
    VOICE_RECORDING,
    WIFI,
]

SURVEY_DATA_FILES = [SURVEY_ANSWERS, SURVEY_TIMINGS]

UPLOAD_FILE_TYPE_MAPPING = {
    "accel": ACCELEROMETER,
    "bluetoothLog": BLUETOOTH,
    "callLog": CALL_LOG,
    "devicemotion": DEVICEMOTION,
    "gps": GPS,
    "gyro": GYRO,
    "logFile": ANDROID_LOG_FILE,
    "magnetometer": MAGNETOMETER,
    "powerState": POWER_STATE,
    "reachability": REACHABILITY,
    "surveyAnswers": SURVEY_ANSWERS,
    "surveyTimings": SURVEY_TIMINGS,
    "textsLog": TEXTS_LOG,
    "voiceRecording": VOICE_RECORDING,
    "wifiLog": WIFI,
    "proximity": PROXIMITY,
    "ios_log": IOS_LOG_FILE,
    "imageSurvey": IMAGE_FILE,
    "ambientAudio": AMBIENT_AUDIO,
    "identifiers": IDENTIFIERS,  # not processed through data upload.
}

# this is mostly used for debugging and scripting
REVERSE_UPLOAD_FILE_TYPE_MAPPING = {v: k for k, v in UPLOAD_FILE_TYPE_MAPPING.items()}

# Used for debugging and reverse lookups.
DATA_STREAM_TO_S3_FILE_NAME_STRING = {
    ACCELEROMETER: "accel",
    BLUETOOTH: "bluetoothLog",
    CALL_LOG: "callLog",
    GPS: "gps",
    IDENTIFIERS: "identifiers",
    ANDROID_LOG_FILE: "logFile",
    POWER_STATE: "powerState",
    SURVEY_ANSWERS: "surveyAnswers",
    SURVEY_TIMINGS: "surveyTimings",
    TEXTS_LOG: "textsLog",
    VOICE_RECORDING: "voiceRecording",
    WIFI: "wifiLog",
    PROXIMITY: "proximity",
    GYRO: "gyro",
    MAGNETOMETER: "magnetometer",
    DEVICEMOTION: "devicemotion",
    REACHABILITY: "reachability",
    IOS_LOG_FILE: "ios/log",
    IMAGE_FILE: "imageSurvey",
    AMBIENT_AUDIO: "ambientAudio",
}

CHUNKABLE_FILES = {
    ACCELEROMETER,
    BLUETOOTH,
    CALL_LOG,
    GPS,
    IDENTIFIERS,
    ANDROID_LOG_FILE,
    POWER_STATE,
    SURVEY_TIMINGS,
    TEXTS_LOG,
    WIFI,
    PROXIMITY,
    GYRO,
    MAGNETOMETER,
    DEVICEMOTION,
    REACHABILITY,
    IOS_LOG_FILE
}


## Survey Question Types
FREE_RESPONSE = "free_response"
CHECKBOX = "checkbox"
RADIO_BUTTON = "radio_button"
SLIDER = "slider"
INFO_TEXT_BOX = "info_text_box"

ALL_QUESTION_TYPES = {
    FREE_RESPONSE,
    CHECKBOX,
    RADIO_BUTTON,
    SLIDER,
    INFO_TEXT_BOX
}

NUMERIC_QUESTIONS = {
    RADIO_BUTTON,
    SLIDER,
    FREE_RESPONSE
}

## Free Response text field types (answer types)
FREE_RESPONSE_NUMERIC = "NUMERIC"
FREE_RESPONSE_SINGLE_LINE_TEXT = "SINGLE_LINE_TEXT"
FREE_RESPONSE_MULTI_LINE_TEXT = "MULTI_LINE_TEXT"

TEXT_FIELD_TYPES = {
    FREE_RESPONSE_NUMERIC,
    FREE_RESPONSE_SINGLE_LINE_TEXT,
    FREE_RESPONSE_MULTI_LINE_TEXT
}

## Comparators
COMPARATORS = {
    "<",
    ">",
    "<=",
    ">=",
    "==",
    "!="
}

NUMERIC_COMPARATORS = {
    "<",
    ">",
    "<=",
    ">="
}

## Password Check Regexes
SYMBOL_REGEX = "[^a-zA-Z0-9]"
LOWERCASE_REGEX = "[a-z]"
UPPERCASE_REGEX = "[A-Z]"
NUMBER_REGEX = "[0-9]"
PASSWORD_REQUIREMENT_REGEX_LIST = [SYMBOL_REGEX, LOWERCASE_REGEX, UPPERCASE_REGEX, NUMBER_REGEX]

# Encryption constants
ASYMMETRIC_KEY_LENGTH = 2048  # length of private/public keys
ITERATIONS = 1000  # number of SHA iterations in password hashing

# Error reporting send-from emails
E500_EMAIL_ADDRESS = 'e500_error@{}'.format(DOMAIN_NAME)
OTHER_EMAIL_ADDRESS = 'telegram_service@{}'.format(DOMAIN_NAME)


# Researcher User Types
class ResearcherRole(object):
    # site_admin = "site_admin"  # site admin is not a study relationship
    study_admin = "study_admin"
    researcher = "study_researcher"


ALL_RESEARCHER_TYPES = (ResearcherRole.study_admin, ResearcherRole.researcher)


PROJECT_ROOT = __file__.rsplit("/", 2)[0] + "/"
PROJECT_PARENT_FOLDER = PROJECT_ROOT.rsplit("/", 2)[0] + "/"


# Celery Constants
DATA_PROCESSING_CELERY_SERVICE = "services.celery_data_processing"
DATA_PROCESSING_CELERY_QUEUE = "data_processing"
PUSH_NOTIFICATION_SEND_SERVICE = "services.push_notification_send"
PUSH_NOTIFICATION_SEND_QUEUE = "push_notifications"
FOREST_SERVICE = "services.celery_forest"
FOREST_QUEUE = "forest_queue"


class ScheduleTypes(object):
    absolute = "absolute"
    relative = "relative"
    weekly = "weekly"

    @classmethod
    def choices(cls):
        return (
            (cls.absolute, "Absolute Schedule"),
            (cls.relative, "Relative Schedule"),
            (cls.weekly, "Weekly Schedule")
        )


# Push notification constants
CELERY_CONFIG_LOCATION = path_join(PROJECT_ROOT, "manager_ip")
ANDROID_FIREBASE_CREDENTIALS = "android_firebase_credentials"
IOS_FIREBASE_CREDENTIALS = "ios_firebase_credentials"
BACKEND_FIREBASE_CREDENTIALS = "backend_firebase_credentials"
# firebase gets the default app name unless otherwise specified, so it is necessary to have
# another name for testing that will never be used to send notifications
FIREBASE_APP_TEST_NAME = 'FIREBASE_APP_TEST_NAME'

DELTA_CHAR = "\u0394"
