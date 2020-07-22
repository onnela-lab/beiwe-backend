import io
import json
import os
import zipfile

from os import path

try:
    import requests
except ImportError as e:
    print(str(e))
    print("\n\nThis scripts uses the python requests library, it is not installed.")
    exit(1)


# Comment out the following import to disable the credentials file.
try:
    from my_data_access_api_credentials import ACCESS_KEY, SECRET_KEY, API_URL_BASE
except ImportError:
    ACCESS_KEY, SECRET_KEY, API_URL_BASE = None, None, ""

# used in commented out code
API_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


# Data Streams
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

RUNNING_IN_TEST_MODE = False
SKIP_DOWNLOAD = False


# correct bad urls for sanity...
# if not API_URL_BASE:
#     print("You need to define the variable API_URL_BASE, this is the domain of your Beiwe deployment.")
#     exit(1)
# if not API_URL_BASE.startswith("https://"):
#     API_URL_BASE = "https://" + API_URL_BASE
if not API_URL_BASE.endswith("/"):
    API_URL_BASE = API_URL_BASE + "/"


def make_request(
        study_id, access_key=ACCESS_KEY, secret_key=SECRET_KEY, user_ids=None, data_streams=None,
        time_start=None, time_end=None
):
    """
    Behavior
    This function will download the data from the server, decompress it, and WRITE IT TO FILES IN
    YOUR CURRENT WORKING DIRECTORY. If the data in the current working directory includes a
    "registry.dat" file, the server will use the contents of it to only download files that are
    new, potentially greatly speeding up your requests.

    Study ID
    study_id is required for any query. The ID of a given study is displayed immediately under
    the name of the study on the study's page on your website. study_id is a string; it will look
    like this: 55f9d1a597013e3f50ffb4c7
    If you know your study's integer identifier you can pass that instead.

    Credentials
    If you are using the my_data_access_credentials.py file your credentials will automatically
    be pulled from it; you will not need to provide those as kwargs. Simply fill in the
    appropriate access and secret key values to the variables in that file. You can find your
    credentials on your website, click the Manage Credentials tab on the top of the page.

    User Ids
    You may provide a list of user IDs as strings to this function, they should be identical to
    the user IDs displayed when viewing a study on your website. Default behavior: if you provide
    no users data will be returned for ALL users in that study.

    Data Streams
    To specify a certain data stream add it to a list and provide that list to the data streams
    folder.  The data streams can be imported from this module, they are ACCELEROMETER,
    BLUETOOTH, CALL_LOG, GPS, IDENTIFIERS, LOG_FILE, POWER_STATE, SURVEY_ANSWERS, SURVEY_TIMINGS,
    TEXTS_LOG, VOICE_RECORDING, and WIFI. Default behavior: if you provide to data streams data
    will be returned for ALL available data streams.

    Dates and Times
    Time-string format: YYYY-MM-DDThh:mm:ss
    (that is an upper case letter T separating the date and the time)
    Example: 1990-01-31T07:30:04 gets you Jan 31 1990 at 7:30:04 AM
    Behavior: the times provided are inclusive, that is you will receive data contained in files
    with an exactly matching time. Default behavior: if you provide no start time parameter data
    will be returned starting from the beginning of time for that user; if you provide no end
    time parameter data will be returned up to the most current indexed data. NOTE: granularity
    of requesting time is by hour, data will be updated on the server roughly once an hour. NOTE:
    Use the string from this module's API_TIME_FORMAT variable if you are using the Python
    DateTime library to generate date strings, or investigate the commented out lines of code in
    this function.
    """

    if access_key is None or secret_key is None:
        raise Exception("You must provide credentials to run this API call.")

    url = API_URL_BASE + 'get-data/v1'
    try:
        int(study_id)
        study_key = "study_pk"
    except ValueError:
        study_key = "study_id"

    values = {
        'access_key': access_key,
        'secret_key': secret_key,
        study_key: study_id,
    }


    if user_ids:
        values['user_ids'] = json.dumps(user_ids)
    if data_streams:
        values['data_streams'] = json.dumps(data_streams)

    # Uncomment the below lines to enable (time zone unaware) datetime object support, add 'from datetime import datetime' to the imports.
    if time_start:
        # if isinstance(time_start, datetime):
        # time_start = time_start.strftime(API_TIME_FORMAT)
        values['time_start'] = time_start
    if time_end:
        # if isinstance(time_end, datetime):
        # time_end = time_end.strftime(API_TIME_FORMAT)
        values['time_end'] = time_end

    if path.exists("master_registry"):
        with open("master_registry") as f:
            old_registry = json.load(f)
            f.close()
            values["registry"] = json.dumps(old_registry)
    else:
        old_registry = {}

    print("sending request, receiving data, this could take some time.")
    response = requests.post(url, data=values)

    if response.status_code != 200:
        raise requests.exceptions.HTTPError(response.status_code)

    if RUNNING_IN_TEST_MODE and SKIP_DOWNLOAD:
        raise requests.exceptions.HTTPError(response.status_code)

    data = response.content
    print("Data received.  Unpacking and overwriting any updated files into", path.abspath('.'))

    z = zipfile.ZipFile(io.BytesIO(data))
    z.extractall()

    with open("registry") as f:
        new_registry = json.load(f)
        f.close()

    old_registry.update(new_registry)
    with open("master_registry", "w") as f:
        json.dump(old_registry, f)
    os.remove("registry")
    print("Operations complete.")
    # Uncomment the following line to have the function return a list of newly updated files.
    # return [name.filename for name in z.filelist if name.filename != "registry"]


def get_users_request(study_id, access_key=ACCESS_KEY, secret_key=SECRET_KEY):
    """ Provides a list of user ids enrolled in the given study. """
    url = API_URL_BASE + 'get-users/v1'
    values = {
        'access_key': access_key,
        'secret_key': secret_key,
        'study_id': study_id,
    }
    data = requests.post(url, data=values).content.decode()
    return json.loads(data)


def get_studies_request(access_key=ACCESS_KEY, secret_key=SECRET_KEY):
    """ Provides a dictionary of the form {study_key:study_name} for studies accessible to the provided user credentials"""
    url = API_URL_BASE + 'get-studies/v1'
    values = {
        'access_key': access_key,
        'secret_key': secret_key,
    }
    data = requests.post(url, data=values).content.decode()
    return json.loads(data)
