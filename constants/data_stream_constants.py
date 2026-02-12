## Constants for for the keys in DATA_STREAM_TO_S3_FILE_NAME_STRING
# TODO: make these part of a class.... that is better than the string lists below

class DataStreams:
    ACCELEROMETER = "accelerometer"
    AI_CHAT_LOGS = "ai_chat_logs"  # not collected on devices, collected through backend
    AUDIO_RECORDING = "audio_recordings"
    ANDROID_LOG_FILE = "app_log"
    BLUETOOTH = "bluetooth"
    CALL_LOG = "calls"
    DEVICEMOTION = "devicemotion"
    GPS = "gps"
    GYRO = "gyro"
    IDENTIFIERS = "identifiers"
    IOS_LOG_FILE = "ios_log"
    MAGNETOMETER = "magnetometer"
    POWER_STATE = "power_state"
    PROXIMITY = "proximity"
    REACHABILITY = "reachability"
    SURVEY_ANSWERS = "survey_answers"
    SURVEY_TIMINGS = "survey_timings"
    TEXTS_LOG = "texts"
    WIFI = "wifi"


ACCELEROMETER = DataStreams.ACCELEROMETER
AI_CHAT_LOGS = DataStreams.AI_CHAT_LOGS
AUDIO_RECORDING = DataStreams.AUDIO_RECORDING
ANDROID_LOG_FILE = DataStreams.ANDROID_LOG_FILE
BLUETOOTH = DataStreams.BLUETOOTH
CALL_LOG = DataStreams.CALL_LOG
DEVICEMOTION = DataStreams.DEVICEMOTION
GPS = DataStreams.GPS
GYRO = DataStreams.GYRO
IDENTIFIERS = DataStreams.IDENTIFIERS
IOS_LOG_FILE = DataStreams.IOS_LOG_FILE
MAGNETOMETER = DataStreams.MAGNETOMETER
POWER_STATE = DataStreams.POWER_STATE
PROXIMITY = DataStreams.PROXIMITY
REACHABILITY = DataStreams.REACHABILITY
SURVEY_ANSWERS = DataStreams.SURVEY_ANSWERS
SURVEY_TIMINGS = DataStreams.SURVEY_TIMINGS
TEXTS_LOG = DataStreams.TEXTS_LOG
WIFI = DataStreams.WIFI


ALL_DATA_STREAMS = (  # these strings are used in chunked files
    DataStreams.ACCELEROMETER,
    DataStreams.AI_CHAT_LOGS,
    DataStreams.AUDIO_RECORDING,
    DataStreams.ANDROID_LOG_FILE,
    DataStreams.BLUETOOTH,
    DataStreams.CALL_LOG,
    DataStreams.DEVICEMOTION,
    DataStreams.GPS,
    DataStreams.GYRO,
    DataStreams.IDENTIFIERS,
    DataStreams.IOS_LOG_FILE,
    DataStreams.MAGNETOMETER,
    DataStreams.POWER_STATE,
    DataStreams.PROXIMITY,
    DataStreams.REACHABILITY,
    DataStreams.SURVEY_ANSWERS,
    DataStreams.SURVEY_TIMINGS,
    DataStreams.TEXTS_LOG,
    DataStreams.WIFI,
)

ALL_DATA_STREAMS_SET = frozenset(ALL_DATA_STREAMS)
assert len(ALL_DATA_STREAMS_SET) == len(ALL_DATA_STREAMS)

SURVEY_DATA_FILES = [SURVEY_ANSWERS, SURVEY_TIMINGS]

UPLOAD_FILE_TYPE_MAPPING = {  # These weird (non-constants) strings are used in uploaded file names
    "accel": DataStreams.ACCELEROMETER,
    "voiceRecording": DataStreams.AUDIO_RECORDING,
    "bluetoothLog": DataStreams.BLUETOOTH,
    "callLog": DataStreams.CALL_LOG,
    "devicemotion": DataStreams.DEVICEMOTION,
    "gps": DataStreams.GPS,
    "gyro": DataStreams.GYRO,
    "logFile": DataStreams.ANDROID_LOG_FILE,
    "magnetometer": DataStreams.MAGNETOMETER,
    "powerState": DataStreams.POWER_STATE,
    "reachability": DataStreams.REACHABILITY,
    "surveyAnswers": DataStreams.SURVEY_ANSWERS,
    "surveyTimings": DataStreams.SURVEY_TIMINGS,
    "textsLog": DataStreams.TEXTS_LOG,
    "wifiLog": DataStreams.WIFI,
    "proximity": DataStreams.PROXIMITY,
    "ios_log": DataStreams.IOS_LOG_FILE,  # I don't know why this one doesn't have a slash
    "identifiers": DataStreams.IDENTIFIERS,  # not processed through data upload.
}

# this is mostly used for debugging and scripting
REVERSE_UPLOAD_FILE_TYPE_MAPPING = {v: k for k, v in UPLOAD_FILE_TYPE_MAPPING.items()}

# Used for debugging and reverse lookups.
DATA_STREAM_TO_S3_FILE_NAME_STRING = {  # These weird (non-constants) strings are used in uploaded file names
    DataStreams.ACCELEROMETER: "accel",
    DataStreams.AI_CHAT_LOGS: DataStreams.AI_CHAT_LOGS,  # not uploaded via the normal mechanism.
    DataStreams.AUDIO_RECORDING: "voiceRecording",
    DataStreams.BLUETOOTH: "bluetoothLog",
    DataStreams.CALL_LOG: "callLog",
    DataStreams.GPS: "gps",
    DataStreams.IDENTIFIERS: "identifiers",
    DataStreams.ANDROID_LOG_FILE: "logFile",
    DataStreams.POWER_STATE: "powerState",
    DataStreams.SURVEY_ANSWERS: "surveyAnswers",
    DataStreams.SURVEY_TIMINGS: "surveyTimings",
    DataStreams.TEXTS_LOG: "textsLog",
    DataStreams.WIFI: "wifiLog",
    DataStreams.PROXIMITY: "proximity",
    DataStreams.GYRO: "gyro",
    DataStreams.MAGNETOMETER: "magnetometer",
    DataStreams.DEVICEMOTION: "devicemotion",
    DataStreams.REACHABILITY: "reachability",
    DataStreams.IOS_LOG_FILE: "ios/log",  # this one has a slash because we screwed up historically and can never change it
}

CHUNKABLE_FILES = frozenset({
    DataStreams.ACCELEROMETER,
    DataStreams.BLUETOOTH,
    DataStreams.CALL_LOG,
    DataStreams.GPS,
    DataStreams.IDENTIFIERS,
    DataStreams.ANDROID_LOG_FILE,
    DataStreams.POWER_STATE,
    DataStreams.SURVEY_TIMINGS,
    DataStreams.TEXTS_LOG,
    DataStreams.WIFI,
    DataStreams.PROXIMITY,
    DataStreams.GYRO,
    DataStreams.MAGNETOMETER,
    DataStreams.DEVICEMOTION,
    DataStreams.REACHABILITY,
    DataStreams.IOS_LOG_FILE
})

# annoyingly long
DEVICE_IDENTIFIERS_HEADER = \
    "patient_id,MAC,phone_number,device_id,device_os,os_version,product,brand,hardware_id,manufacturer,model,beiwe_version\n"


## Dashboard constants

DASHBOARD_DATA_STREAMS = (
    DataStreams.ACCELEROMETER,
    DataStreams.AI_CHAT_LOGS,
    DataStreams.ANDROID_LOG_FILE, 
    DataStreams.BLUETOOTH,
    DataStreams.CALL_LOG,
    DataStreams.DEVICEMOTION,
    DataStreams.GPS,
    DataStreams.GYRO,
    DataStreams.IDENTIFIERS,
    DataStreams.IOS_LOG_FILE,
    DataStreams.MAGNETOMETER,
    DataStreams.POWER_STATE,
    DataStreams.PROXIMITY,
    DataStreams.REACHABILITY,
    DataStreams.SURVEY_ANSWERS,
    DataStreams.SURVEY_TIMINGS,
    DataStreams.TEXTS_LOG,
    DataStreams.AUDIO_RECORDING,
    DataStreams.WIFI,
)

COMPLETE_DATA_STREAM_DICT = {
    DataStreams.ACCELEROMETER: "Accelerometer",
    DataStreams.AUDIO_RECORDING: "Audio Recordings",
    DataStreams.AI_CHAT_LOGS: "AI Chat Logs",
    DataStreams.ANDROID_LOG_FILE: "Android Log File",
    DataStreams.BLUETOOTH: "Bluetooth",
    DataStreams.CALL_LOG: "Call Log",
    DataStreams.DEVICEMOTION: "Device Motion",
    DataStreams.GPS: "GPS",
    DataStreams.GYRO: "Gyro",
    DataStreams.IDENTIFIERS: "Identifiers",
    DataStreams.IOS_LOG_FILE: "iOS Log File",
    DataStreams.MAGNETOMETER: "Magnetometer",
    DataStreams.POWER_STATE: "Power State",
    DataStreams.PROXIMITY: "Proximity",
    DataStreams.REACHABILITY: "Reachability",
    DataStreams.SURVEY_ANSWERS: "Survey Answers",
    DataStreams.SURVEY_TIMINGS: "Survey Timings",
    DataStreams.TEXTS_LOG: "Text Log",
    DataStreams.WIFI: "Wifi",
}
