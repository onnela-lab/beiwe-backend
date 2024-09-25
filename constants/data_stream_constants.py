## Constants for for the keys in DATA_STREAM_TO_S3_FILE_NAME_STRING
ACCELEROMETER = "accelerometer"
AMBIENT_AUDIO = "ambient_audio"
ANDROID_LOG_FILE = "app_log"
BLUETOOTH = "bluetooth"
OMNIRING = "omniring"
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
VOICE_RECORDING = "audio_recordings"
WIFI = "wifi"


ALL_DATA_STREAMS = [
    ACCELEROMETER,
    AMBIENT_AUDIO,
    ANDROID_LOG_FILE,
    BLUETOOTH,
    OMNIRING,
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
    VOICE_RECORDING,
    WIFI,
]

SURVEY_DATA_FILES = [SURVEY_ANSWERS, SURVEY_TIMINGS]

UPLOAD_FILE_TYPE_MAPPING = {
    "accel": ACCELEROMETER,
    "bluetoothLog": BLUETOOTH,
    "omniring": OMNIRING,
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
    "ambientAudio": AMBIENT_AUDIO,
    "identifiers": IDENTIFIERS,  # not processed through data upload.
}

# this is mostly used for debugging and scripting
REVERSE_UPLOAD_FILE_TYPE_MAPPING = {v: k for k, v in UPLOAD_FILE_TYPE_MAPPING.items()}

# Used for debugging and reverse lookups.
DATA_STREAM_TO_S3_FILE_NAME_STRING = {
    ACCELEROMETER: "accel",
    BLUETOOTH: "bluetoothLog",
    OMNIRING: "omniring",
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
    AMBIENT_AUDIO: "ambientAudio",
}

CHUNKABLE_FILES = {
    ACCELEROMETER,
    BLUETOOTH,
    OMNIRING,
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
    OMNIRING,
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
    VOICE_RECORDING,
    WIFI,
]

COMPLETE_DATA_STREAM_DICT = {
    ACCELEROMETER: "Accelerometer (bytes)",
    AMBIENT_AUDIO: "Ambient Audio Recording (bytes)",
    ANDROID_LOG_FILE: "Android Log File (bytes)",
    BLUETOOTH: "Bluetooth (bytes)",
    OMNIRING: "Omniring (bytes)",
    CALL_LOG: "Call Log (bytes)",
    DEVICEMOTION: "Device Motion (bytes)",
    GPS: "GPS (bytes)",
    GYRO: "Gyro (bytes)",
    IDENTIFIERS: "Identifiers (bytes)",
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