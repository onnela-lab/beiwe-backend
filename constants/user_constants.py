# participant device os
from config.settings import DATA_DELETION_USERTYPE


IOS_API = "IOS"
ANDROID_API = "ANDROID"
NULL_OS = ''

OS_TYPE_CHOICES = (
    (IOS_API, IOS_API),
    (ANDROID_API, ANDROID_API),
    (NULL_OS, NULL_OS),
)


IOS_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION = "2024.27"


# Researcher User Types
class ResearcherRole:
    study_admin = "study_admin"
    researcher = "study_researcher"
    # site_admin is not a study _relationship_, but we need a canonical string for it somewhere.
    # You are a site admin if 'site_admin' is true on your Researcher model.
    site_admin = "site_admin"
    no_access = "no_access"


if DATA_DELETION_USERTYPE == ResearcherRole.researcher:
    DATA_DELETION_ALLOWED_RELATIONS = (ResearcherRole.researcher, ResearcherRole.study_admin)
elif DATA_DELETION_USERTYPE == ResearcherRole.study_admin:
    DATA_DELETION_ALLOWED_RELATIONS = (ResearcherRole.study_admin, )
elif DATA_DELETION_USERTYPE == ResearcherRole.site_admin:
    DATA_DELETION_ALLOWED_RELATIONS = tuple()
else:
    raise Exception(f"DATA_DELETION_USERTYPE is set to an invalid value: {DATA_DELETION_USERTYPE}")

ALL_RESEARCHER_TYPES = (ResearcherRole.study_admin, ResearcherRole.researcher)


# researcher session constants
SESSION_NAME = "researcher_username"
EXPIRY_NAME = "expiry"
SESSION_UUID = "session_uuid"
SESSION_TIMEOUT_HOURS = 2

# These fields are used to indicate that a participant is still "active", active is defined as
# is still hitting the backend in the passed *insert your time period here*.
# Don't forget that you need to query the AppHeartbeat model to get the last time the app heartbeat.
ACTIVE_PARTICIPANT_FIELDS = (
    'last_upload',
    'last_get_latest_surveys',
    'last_set_password',
    'last_set_fcm_token',
    'last_get_latest_device_settings',
    'last_register_user',
    "last_heartbeat_checkin",
    "permanently_retired",
)
# Don't forget that you need to query the AppHeartbeat model to get the last time the app heartbeat.

# used to determine whether a participant is considered "active"
PARTICIPANT_STATUS_QUERY_FIELDS = (
    "id",  # the database id, use is clear in the code, not actually part of activeness
    "created_on",
    "patient_id",
    "registered",
    "os_type",
    "last_upload",
    "last_get_latest_surveys",
    "last_set_password",
    "last_set_fcm_token",
    "last_get_latest_device_settings",
    "last_register_user",
    "permanently_retired",
    "last_heartbeat_checkin",
)

# used in the participant table api/page content
BASE_TABLE_FIELDS = ["Created On", "Patient ID", "Status", "OS Type"]
EXTRA_TABLE_FIELDS = {
    "first_register_user": "First Registration Date",
    "last_register_user": "Last Registration",
    "last_upload": "Last Upload",
    "last_get_latest_surveys": "Last Survey Download",
    "last_set_password": "Last Set Password",
    "last_set_fcm_token": "Last Push Token Update",
    "last_get_latest_device_settings": "Last Device Settings Update",
    "last_os_version": "Last OS Version",
    "last_version_code": "App Version Code",
    "last_version_name": "App Version Name",
    "last_heartbeat_checkin": "Last Heartbeat",
}
