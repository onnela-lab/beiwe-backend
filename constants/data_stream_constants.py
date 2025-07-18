## Constants for for the keys in DATA_STREAM_TO_S3_FILE_NAME_STRING
ACCELEROMETER = "accelerometer"
AMBIENT_AUDIO = "ambient_audio"
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


ALL_DATA_STREAMS = [
    ACCELEROMETER,
    AUDIO_RECORDING,
    AMBIENT_AUDIO,
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
]

ALL_DATA_STREAMS_SET = set(ALL_DATA_STREAMS)
assert(len(ALL_DATA_STREAMS_SET) == len(ALL_DATA_STREAMS))

SURVEY_DATA_FILES = [SURVEY_ANSWERS, SURVEY_TIMINGS]

UPLOAD_FILE_TYPE_MAPPING = {
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
    "ios_log": IOS_LOG_FILE,
    "ambientAudio": AMBIENT_AUDIO,
    "identifiers": IDENTIFIERS,  # not processed through data upload.
}

# this is mostly used for debugging and scripting
REVERSE_UPLOAD_FILE_TYPE_MAPPING = {v: k for k, v in UPLOAD_FILE_TYPE_MAPPING.items()}

# Used for debugging and reverse lookups.
DATA_STREAM_TO_S3_FILE_NAME_STRING = {
    ACCELEROMETER: "accel",
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
    IOS_LOG_FILE: "ios/log",
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

# annoyingly long
DEVICE_IDENTIFIERS_HEADER = \
    "patient_id,MAC,phone_number,device_id,device_os,os_version,product,brand,hardware_id,manufacturer,model,beiwe_version\n"


## Dashboard constants

DASHBOARD_DATA_STREAMS = [
    ACCELEROMETER,
    AMBIENT_AUDIO,
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
]

COMPLETE_DATA_STREAM_DICT = {
    ACCELEROMETER: "Accelerometer",
    AUDIO_RECORDING: "Audio Recordings",
    AMBIENT_AUDIO: "Ambient Audio",
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