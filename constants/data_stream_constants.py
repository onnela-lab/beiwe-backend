## Constants for for the keys in DATA_STREAM_TO_S3_FILE_NAME_STRING
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


ALL_DATA_STREAMS = (  # these strings are used in chunked files
    ACCELEROMETER,
    AI_CHAT_LOGS,
    AUDIO_RECORDING,
    ANDROID_LOG_FILE,
    BLUETOOTH,
    CALL_LOG,
    DEVICEMOTION,
    GPS,
    GYRO,
    IDENTIFIERS,
    IOS_LOG_FILE,
    MAGNETOMETER,
    POWER_STATE,
    PROXIMITY,
    REACHABILITY,
    SURVEY_ANSWERS,
    SURVEY_TIMINGS,
    TEXTS_LOG,
    WIFI,
)

ALL_DATA_STREAMS_SET = frozenset(ALL_DATA_STREAMS)
assert len(ALL_DATA_STREAMS_SET) == len(ALL_DATA_STREAMS)

SURVEY_DATA_FILES = [SURVEY_ANSWERS, SURVEY_TIMINGS]

UPLOAD_FILE_TYPE_MAPPING = {  # These weird (non-constants) strings are used in uploaded file names
    "accel": ACCELEROMETER,
    "voiceRecording": AUDIO_RECORDING,
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
    "wifiLog": WIFI,
    "proximity": PROXIMITY,
    "ios_log": IOS_LOG_FILE,  # I don't know why this one doesn't have a slash
    "identifiers": IDENTIFIERS,  # not processed through data upload.
}

# this is mostly used for debugging and scripting
REVERSE_UPLOAD_FILE_TYPE_MAPPING = {v: k for k, v in UPLOAD_FILE_TYPE_MAPPING.items()}

# Used for debugging and reverse lookups.
DATA_STREAM_TO_S3_FILE_NAME_STRING = {  # These weird (non-constants) strings are used in uploaded file names
    ACCELEROMETER: "accel",
    AI_CHAT_LOGS: AI_CHAT_LOGS,  # AI_CHAT_LOGS are not uploaded via the normal mechanism
    AUDIO_RECORDING: "voiceRecording",
    BLUETOOTH: "bluetoothLog",
    CALL_LOG: "callLog",
    GPS: "gps",
    IDENTIFIERS: "identifiers",
    ANDROID_LOG_FILE: "logFile",
    POWER_STATE: "powerState",
    SURVEY_ANSWERS: "surveyAnswers",
    SURVEY_TIMINGS: "surveyTimings",
    TEXTS_LOG: "textsLog",
    WIFI: "wifiLog",
    PROXIMITY: "proximity",
    GYRO: "gyro",
    MAGNETOMETER: "magnetometer",
    DEVICEMOTION: "devicemotion",
    REACHABILITY: "reachability",
    IOS_LOG_FILE: "ios/log",  # this one has a slash because we screwed up historically and can never change it
}

CHUNKABLE_FILES = frozenset({
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
})

# annoyingly long
DEVICE_IDENTIFIERS_HEADER = \
    "patient_id,MAC,phone_number,device_id,device_os,os_version,product,brand,hardware_id,manufacturer,model,beiwe_version\n"


## Dashboard constants

DASHBOARD_DATA_STREAMS = (
    ACCELEROMETER,
    AI_CHAT_LOGS,
    ANDROID_LOG_FILE,
    BLUETOOTH,
    CALL_LOG,
    DEVICEMOTION,
    GPS,
    GYRO,
    IDENTIFIERS,
    IOS_LOG_FILE,
    MAGNETOMETER,
    POWER_STATE,
    PROXIMITY,
    REACHABILITY,
    SURVEY_ANSWERS,
    SURVEY_TIMINGS,
    TEXTS_LOG,
    AUDIO_RECORDING,
    WIFI,
)

COMPLETE_DATA_STREAM_DICT = {
    ACCELEROMETER: "Accelerometer",
    AUDIO_RECORDING: "Audio Recordings",
    AI_CHAT_LOGS: "AI Chat Logs",
    ANDROID_LOG_FILE: "Android Log File",
    BLUETOOTH: "Bluetooth",
    CALL_LOG: "Call Log",
    DEVICEMOTION: "Device Motion",
    GPS: "GPS",
    GYRO: "Gyro",
    IDENTIFIERS: "Identifiers",
    IOS_LOG_FILE: "iOS Log File",
    MAGNETOMETER: "Magnetometer",
    POWER_STATE: "Power State",
    PROXIMITY: "Proximity",
    REACHABILITY: "Reachability",
    SURVEY_ANSWERS: "Survey Answers",
    SURVEY_TIMINGS: "Survey Timings",
    TEXTS_LOG: "Text Log",
    WIFI: "Wifi",
}
