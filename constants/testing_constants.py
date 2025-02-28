from datetime import datetime

from constants.common_constants import EASTERN
from constants.user_constants import ResearcherRole


HOST = "localhost.localdomain"
PORT = 54321

BASE_URL = f"http://{HOST}:{PORT}"
TEST_PASSWORD = "1"
TEST_STUDY_NAME = "automated_test_study"
TEST_STUDY_ENCRYPTION_KEY = "11111111111111111111111111111111"
TEST_USERNAME = "automated_test_user"

# ALL_ROLE_PERMUTATIONS is generated from this:
# ALL_ROLE_PERMUTATIONS = tuple(
# from constants.user_constants import ResearcherRole
# from itertools import permutations
#     two_options for two_options in permutations(
#     (ResearcherRole.site_admin, ResearcherRole.study_admin, ResearcherRole.researcher, None), 2)
# )

ALL_ROLE_PERMUTATIONS = (
    ('site_admin', 'study_admin'),
    ('site_admin', 'study_researcher'),
    ('site_admin', None),
    ('study_admin', 'site_admin'),
    ('study_admin', 'study_researcher'),
    ('study_admin', None),
    ('study_researcher', 'site_admin'),
    ('study_researcher', 'study_admin'),
    ('study_researcher', None),
    (None, 'site_admin'),
    (None, 'study_admin'),
    (None, 'study_researcher'),
)

REAL_ROLES = (ResearcherRole.study_admin, ResearcherRole.researcher)
ALL_TESTING_ROLES = (ResearcherRole.study_admin, ResearcherRole.researcher, ResearcherRole.site_admin, None)
ADMIN_ROLES = (ResearcherRole.study_admin, ResearcherRole.site_admin)


BACKEND_CERT = """{
    "type": "service_account",
    "project_id": "some id",
    "private_key_id": "numbers and letters",
    "private_key": "-----BEGIN PRIVATE KEY-----omg a key-----END PRIVATE KEY-----",
    "client_email": "firebase-adminsdk *serviceaccountinfo*",
    "client_id": "NUMBERS!",
    "auth_uri": "https://an_account_oauth",
    "token_uri": "https://an_account/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "some_neato_cert_url"
}"""


IOS_CERT = \
"""<?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" 
    "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
    <plist version="1.0">
    <dict>
    <key>CLIENT_ID</key>
    <string>some url id</string>
    <key>REVERSED_CLIENT_ID</key>
    <string>id url some</string>
    <key>API_KEY</key>
    <string>gibberish</string>
    <key>GCM_SENDER_ID</key>
    <string>number junk</string>
    <key>PLIST_VERSION</key>
    <string>1</string>
    <key>BUNDLE_ID</key>
    <string>an bundle eye dee</string>
    <key>PROJECT_ID</key>
    <string>name with a number</string>
    <key>STORAGE_BUCKET</key>
    <string>something dot appspot.com</string>
    <key>IS_ADS_ENABLED</key>
    <false></false>
    <key>IS_ANALYTICS_ENABLED</key>
    <false></false>
    <key>IS_APPINVITE_ENABLED</key>
    <true></true>
    <key>IS_GCM_ENABLED</key>
    <true></true>
    <key>IS_SIGNIN_ENABLED</key>
    <true></true>
    <key>GOOGLE_APP_ID</key>
    <string>obscure base64 with colon separaters</string>
    <key>DATABASE_URL</key>
    <string>https://something.firebaseio.com</string>
    </dict>
    </plist>"""

ANDROID_CERT = """{
"project_info": {
    "project_number": "an large number",
    "firebase_url": "https://some_identifier.firebaseio.com",
    "project_id": "some_identifier",
    "storage_bucket": "some_identifier.appspot.com"},
"client": [{
    "client_info": {
    "mobilesdk_app_id": "inscrutable colon separated bas64",
    "android_client_info": {"package_name": "org.beiwe.app"}
    },
    "oauth_client": [
    {"client_id": "some_client_id",
    "client_type": 3}
    ],
    "api_key": [{"current_key": "a key!"}],
    "services": {
    "appinvite_service": {
        "other_platform_oauth_client": [
        {"client_id": "some_client_id", "client_type": 3},
        {"client_id": "more.junk.apps.googleusercontent.com",
        "client_type": 2, "ios_info": {"bundle_id": "an_bundle_id"}}
    ]}
    }
}, {"client_info": {
    "mobilesdk_app_id": "inscrutable colon separated bas64",
    "android_client_info": {"package_name": "package name!"}
    },
    "oauth_client": [
        {"client_id": "some_client_id", "client_type": 3}
    ],
    "api_key": [{"current_key": "base64 junk"}],
    "services": {
        "appinvite_service": {
        "other_platform_oauth_client": [{
            "client_id": "some_client_id",
            "client_type": 3
            },{
            "client_id": "some-identifier.apps.googleusercontent.com",
            "client_type": 2,
            "ios_info": {"bundle_id": "another bundle id"}
            }
        ]}
    }
    }],
"configuration_version": "1"
}"""


def MIDNIGHT_EVERY_DAY_OF_WEEK():
    return [[0], [0], [0], [0], [0], [0], [0]]


def NOON_EVERY_DAY_OF_WEEK():
    return [[43200], [43200], [43200], [43200], [43200], [43200], [43200]]


# we need some moments in space-time, so I guess we will use October Thursdays in New York of 2022
THURS_OCT_6_NOON_2022_NY = datetime(2022, 10, 6, 12, tzinfo=EASTERN)  # Thursday
THURS_OCT_13_NOON_2022_NY = datetime(2022, 10, 13, 12, tzinfo=EASTERN)  # Thursday
THURS_OCT_20_NOON_2022_NY = datetime(2022, 10, 20, 12, tzinfo=EASTERN)  # Thursday
THURS_OCT_27_NOON_2022_NY = datetime(2022, 10, 27, 12, tzinfo=EASTERN)  # Thursday


# a whole week starting on a Monday June 5th 2022 during EASTERN DAYLIGHT TIME
MONDAY_JUNE_NOON_6_2022_EDT = datetime(2022, 6, 6, 12, tzinfo=EASTERN)  # Monday
TUESDAY_JUNE_NOON_7_2022_EDT = datetime(2022, 6, 7, 12, tzinfo=EASTERN)  # Tuesday
WEDNESDAY_JUNE_NOON_8_2022_EDT = datetime(2022, 6, 8, 12, tzinfo=EASTERN)  # Wednesday
THURSDAY_JUNE_NOON_9_2022_EDT = datetime(2022, 6, 9, 12, tzinfo=EASTERN)  # Thursday
FRIDAY_JUNE_NOON_10_2022_EDT = datetime(2022, 6, 10, 12, tzinfo=EASTERN)  # Friday
SATURDAY_JUNE_NOON_11_2022_EDT = datetime(2022, 6, 11, 12, tzinfo=EASTERN)  # Saturday
SUNDAY_JUNE_NOON_12_2022_EDT = datetime(2022, 6, 12, 12, tzinfo=EASTERN)  # Sunday

# a whole week starting Monday January 9th 2022 during EASTERN DAYLIGHT SAVINGS TIME
MONDAY_JAN_10_NOON_2022_EST = datetime(2022, 1, 10, 12, tzinfo=EASTERN)  # Monday
TUESDAY_JAN_11_NOON_2022_EST = datetime(2022, 1, 11, 12, tzinfo=EASTERN)  # Tuesday
WEDNESDAY_JAN_12_NOON_2022_EST = datetime(2022, 1, 12, 12, tzinfo=EASTERN)  # Wednesday
THURSDAY_JAN_13_NOON_2022_EST = datetime(2022, 1, 13, 12, tzinfo=EASTERN)  # Thursday
FRIDAY_JAN_14_NOON_2022_EST = datetime(2022, 1, 14, 12, tzinfo=EASTERN)  # Friday
SATURDAY_JAN_15_NOON_2022_EST = datetime(2022, 1, 15, 12, tzinfo=EASTERN)  # Saturday
SUNDAY_JAN_16_NOON_2022_EST = datetime(2022, 1, 16, 12, tzinfo=EASTERN)  # Sunday

EDT_WEEK = [
    MONDAY_JUNE_NOON_6_2022_EDT,
    TUESDAY_JUNE_NOON_7_2022_EDT,
    WEDNESDAY_JUNE_NOON_8_2022_EDT,
    THURSDAY_JUNE_NOON_9_2022_EDT,
    FRIDAY_JUNE_NOON_10_2022_EDT,
    SATURDAY_JUNE_NOON_11_2022_EDT,
    SUNDAY_JUNE_NOON_12_2022_EDT,
]

EST_WEEK = [
    MONDAY_JAN_10_NOON_2022_EST,
    TUESDAY_JAN_11_NOON_2022_EST,
    WEDNESDAY_JAN_12_NOON_2022_EST,
    THURSDAY_JAN_13_NOON_2022_EST,
    FRIDAY_JAN_14_NOON_2022_EST,
    SATURDAY_JAN_15_NOON_2022_EST,
    SUNDAY_JAN_16_NOON_2022_EST,
]


## these are variables for use in tests that deal with the data access api code. Its a bit haphazard.
# this includes the byte indicator of a zip file and no data, e.g. it is an empty zip file.
EMPTY_ZIP = b'PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
SIMPLE_FILE_CONTENTS = b"this is the file content you are looking for"
