import json

## These are all used on various study pages

ABOUT_PAGE_TEXT = (
    'The Beiwe application runs on your phone and helps researchers collect information about your '
    'behaviors. Beiwe may ask you to fill out short surveys or to record your voice. It may collect '
    'information about your location (using phone GPS) and how much you move (using phone '
    'accelerometer). Beiwe may also monitor how much you use your phone for calling and texting and '
    'keep track of the people you communicate with. Importantly, Beiwe never records the names or '
    'phone numbers of anyone you communicate with. While it can tell if you call the same person '
    'more than once, it does not know who that person is. Beiwe also does not record the content of '
    'your text messages or phone calls. Beiwe may keep track of the different Wi-Fi networks and '
    'Bluetooth devices around your phone, but the names of those networks are replaced with random '
    'codes.\n\nAlthough Beiwe collects large amounts of data, the data is processed to protect your '
    'privacy. This means that it does not know your name, your phone number, or anything else that '
    'could identify you. Beiwe only knows you by an identification number. Because Beiwe does not '
    'know who you are, it cannot communicate with your clinician if you are ill or in danger. '
    'Researchers will not review the data Beiwe collects until the end of the study. To make it '
    'easier for you to connect with your clinician, the \'Call my Clinician\' button appears at the '
    'bottom of every page.\n\nBeiwe was conceived and designed by Dr. Jukka-Pekka \'JP\' Onnela at '
    'the Harvard T.H. Chan School of Public Health. Development of the Beiwe smartphone application '
    'and data analysis software is funded by NIH grant 1DP2MH103909-01 to Dr. Onnela. The smartphone '
    'application was built by Zagaran, Inc., in Cambridge, Massachusetts.'
)

CONSENT_FORM_TEXT = (
    'I have read and understood the information about the study and all of my questions about the '
    'study have been answered by the study researchers.'
)

SURVEY_SUBMIT_SUCCESS_TOAST_TEXT = (
    'Thank you for completing the survey. A clinician will not see your answers immediately, so '
    'if you need help or are thinking about harming yourself, please contact your clinician. You '
    'can also press the \'Call My Clinician\' button.'
)

DEFAULT_CONSENT_SECTIONS = {
    "welcome": {"text": "", "more": ""},
    "data_gathering": {"text": "", "more": ""},
    "privacy": {"text": "", "more": ""},
    "data_use": {"text": "", "more": ""},
    "time_commitment": {"text": "", "more": ""},
    "study_survey": {"text": "", "more": ""},
    "study_tasks": {"text": "", "more": ""},
    "withdrawing": {"text": "", "more": ""}
}
DEFAULT_CONSENT_SECTIONS_JSON = json.dumps(DEFAULT_CONSENT_SECTIONS)

AUDIO_SURVEY_SETTINGS = {
    'audio_survey_type': 'compressed',
    'bit_rate': 64000,
    'sample_rate': 44100,
}

CHECKBOX_TOGGLES = [
    "accelerometer",
    "gps",
    "calls",
    "texts",
    "wifi",
    "bluetooth",
    "omniring",
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
    "omniring_off_duration_seconds",
    "omniring_on_duration_seconds",
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
    "devicemotion_on_duration_seconds",
    "heartbeat_timer_minutes",
]

# Surveys have several types of questions and some special symbols

# Survey Question Types
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
    FREE_RESPONSE,
    CHECKBOX,
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
