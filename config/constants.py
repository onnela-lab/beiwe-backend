from os import getenv

from config.settings import DOMAIN_NAME

### Environment settings ###
# All settings here can be configured by setting an environment variable, or by editing the default value

# To customize any of these values, use the following pattern.
# DEFAULT_S3_RETRIES = getenv("DEFAULT_S3_RETRIES") or 10
# Note that this file is _not_ in the gitignore.

## Networking
#This value is used in libs.s3, does what it says.
DEFAULT_S3_RETRIES = getenv("DEFAULT_S3_RETRIES") or 3

## File processing directives
#NOTE: these numbers were determined through trial and error on a C4 Large AWS instance.
#Used in data download and data processing, base this on CPU core count.
CONCURRENT_NETWORK_OPS = getenv("CONCURRENT_NETWORK_OPS") or 10
#Used in file processing, number of files to be pulled in and processed simultaneously.
# Higher values reduce s3 usage, reduce processing time, but increase ram requirements.
FILE_PROCESS_PAGE_SIZE = getenv("FILE_PROCESS_PAGE_SIZE") or 250

#This string will be printed into non-error hourly reports to improve error filtering.
DATA_PROCESSING_NO_ERROR_STRING = getenv("DATA_PROCESSING_NO_ERROR_STRING") or "2HEnBwlawY"

# The number of minutes after which a queued celery task will be invalidated.
# (this is not a timeout, it only invalidates tasks that have not yet run.)
CELERY_EXPIRY_MINUTES = getenv("CELERY_EXPIRY_MINUTES") or 4
CELERY_ERROR_REPORT_TIMEOUT_SECONDS = getenv("CELERY_ERROR_REPORT_TIMEOUT_SECONDS") or 60*15


## Data streams and survey types ##
ALLOWED_EXTENSIONS = {'csv', 'json', 'mp4', "wav", 'txt', 'jpg'}
PROCESSABLE_FILE_EXTENSIONS = [".csv", ".mp4", ".wav"]
# These don't appear to be used...
MEDIA_EXTENSIONS = [".mp4", ".wav", ".jpg"]
FILE_TYPES = ['gps', 'accel', 'voiceRecording', 'powerState', 'callLog', 'textLog',
              'bluetoothLog', 'surveyAnswers', 'surveyTimings', 'imageSurvey']


## HTML lists ##
CHECKBOX_TOGGLES = ["accelerometer",
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
                    "reachability",
                    "allow_upload_over_cellular_data",
                    "use_anonymized_hashing",
                    "use_gps_fuzzing",
                    "call_clinician_button_enabled",
                    "call_research_assistant_button_enabled"]

TIMER_VALUES = ["accelerometer_off_duration_seconds",
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
                "devicemotion_on_duration_seconds" ]

# The format that dates should be in throughout the codebase
API_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
"""1990-01-31T07:30:04 gets you jan 31 1990 at 7:30:04am
   human string is YYYY-MM-DDThh:mm:ss """

# used in the dashboard
REDUCED_API_TIME_FORMAT = "%Y-%m-%d"

## Chunks
# This value is in seconds, it sets the time period that chunked files will be sliced into.
CHUNK_TIMESLICE_QUANTUM = 3600
# the name of the s3 folder that contains chunked data
CHUNKS_FOLDER = "CHUNKED_DATA"
PIPELINE_FOLDER = "PIPELINE_DATA"

## Constants for for the keys in data_stream_to_s3_file_name_string
ACCELEROMETER = "accelerometer"
BLUETOOTH = "bluetooth"
CALL_LOG = "calls"
GPS = "gps"
IDENTIFIERS = "identifiers"
ANDROID_LOG_FILE = "app_log"
IOS_LOG_FILE = "ios_log"
POWER_STATE = "power_state"
SURVEY_ANSWERS = "survey_answers"
SURVEY_TIMINGS = "survey_timings"
TEXTS_LOG = "texts"
VOICE_RECORDING = "audio_recordings"
IMAGE_FILE = "image_survey"
WIFI = "wifi"
PROXIMITY = "proximity"
GYRO = "gyro"
MAGNETOMETER = "magnetometer"
DEVICEMOTION = "devicemotion"
REACHABILITY = "reachability"

# dictionary for printing PROCESSED data streams for frontend
# necessary even with the complete data stream dict for parsing data from the backend
processed_data_stream_dict = {
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
complete_data_stream_dict = {
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


ALL_DATA_STREAMS = [ACCELEROMETER,
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
    "imageSurvey":IMAGE_FILE,
}


def data_stream_to_s3_file_name_string(data_type):
    """Maps a data type to the internal string representation used throughout the codebase.
        (could be a dict mapping, but it is fine) """
    if data_type == ACCELEROMETER: return "accel"
    if data_type == BLUETOOTH: return "bluetoothLog"
    if data_type == CALL_LOG: return "callLog"
    if data_type == GPS: return "gps"
    if data_type == IDENTIFIERS: return "identifiers"
    if data_type == ANDROID_LOG_FILE: return "logFile"
    if data_type == POWER_STATE: return "powerState"
    if data_type == SURVEY_ANSWERS: return "surveyAnswers"
    if data_type == SURVEY_TIMINGS: return "surveyTimings"
    if data_type == TEXTS_LOG: return "textsLog"
    if data_type == VOICE_RECORDING: return "voiceRecording"
    if data_type == WIFI: return "wifiLog"
    if data_type == PROXIMITY: return "proximity"
    if data_type == GYRO: return "gyro"
    if data_type == MAGNETOMETER: return "magnetometer"
    if data_type == DEVICEMOTION: return "devicemotion"
    if data_type == REACHABILITY: return "reachability"
    if data_type == IOS_LOG_FILE: return "ios_log"
    if data_type == IMAGE_FILE: "imageSurvey"
    raise Exception("unknown data type: %s" % data_type)

CHUNKABLE_FILES = {ACCELEROMETER,
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
                   IOS_LOG_FILE}

## Survey Question Types
FREE_RESPONSE = "free_response"
CHECKBOX = "checkbox"
RADIO_BUTTON = "radio_button"
SLIDER = "slider"
INFO_TEXT_BOX = "info_text_box"

ALL_QUESTION_TYPES = {FREE_RESPONSE,
                      CHECKBOX,
                      RADIO_BUTTON,
                      SLIDER,
                      INFO_TEXT_BOX }

NUMERIC_QUESTIONS = {RADIO_BUTTON,
                     SLIDER,
                     FREE_RESPONSE }

## Free Response text field types (answer types)
FREE_RESPONSE_NUMERIC = "NUMERIC"
FREE_RESPONSE_SINGLE_LINE_TEXT = "SINGLE_LINE_TEXT"
FREE_RESPONSE_MULTI_LINE_TEXT = "MULTI_LINE_TEXT"

TEXT_FIELD_TYPES = {FREE_RESPONSE_NUMERIC,
                    FREE_RESPONSE_SINGLE_LINE_TEXT,
                    FREE_RESPONSE_MULTI_LINE_TEXT }

## Comparators
COMPARATORS = {"<",
               ">",
               "<=",
               ">=",
               "==",
               "!=" }

NUMERIC_COMPARATORS = {"<",
                       ">",
                       "<=",
                       ">=" }

## Password Check Regexes
SYMBOL_REGEX = "[^a-zA-Z0-9]"
LOWERCASE_REGEX = "[a-z]"
UPPERCASE_REGEX = "[A-Z]"
NUMBER_REGEX = "[0-9]"
PASSWORD_REQUIREMENT_REGEX_LIST = [SYMBOL_REGEX, LOWERCASE_REGEX, UPPERCASE_REGEX, NUMBER_REGEX]

DEVICE_IDENTIFIERS_HEADER = "patient_id,MAC,phone_number,device_id,device_os,os_version,product,brand,hardware_id,manufacturer,model,beiwe_version\n"

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
