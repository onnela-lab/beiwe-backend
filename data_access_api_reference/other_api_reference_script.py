from datetime import datetime

# You need 2 libraries installed to run this script, run `pip install orjson requests`
# requests is a (fantastic) library for making http requests
# orjson is a highly optimized library for parsing json, I assure you, you will want to use it.
import orjson
import requests


# provide your Beiwe access keys here
ACCESS_KEY = "your access key"
SECRET_KEY = "your secret key"

# Enter the top-level component of your Beiwe server.
MY_BEIWE_SERVER = "https://example.com"

# these are the current assortment of endpoints, other than get-data/v1.
# TARGET_ENDPOINT_URL = f"{MY_BEIWE_SERVER}/get-studies/v1"
    # Endpoint takes no parameters, returns a json dictionary of study ids and their study name.
    # Returns the studies that the user has access to.

# TARGET_ENDPOINT_URL = f"{MY_BEIWE_SERVER}/get-participant-ids/v1"
# TARGET_ENDPOINT_URL = f"{MY_BEIWE_SERVER}/get-users/v1"  # deprecated, use get-participant-ids
    # Endpoint takes one parameter, study_id, returns a json list of participant ids for that study.
    # This endpoint is provisionally deprecated, it is not guaranteed to be available in the future.

# TARGET_ENDPOINT_URL = f"{MY_BEIWE_SERVER}/get-participant-data-quantities/v1"
    # Endpoint takes one parameter, study_id, returns complex json of participants and their data
    # quantity metrics as reported on the dashboard.

# TARGET_ENDPOINT_URL = f"{MY_BEIWE_SERVER}/get-interventions/v1"
    # Endpoint takes one parameter, study_id, returns complex json of participants and their
    # interventions, including dates and times

# TARGET_ENDPOINT_URL = f"{MY_BEIWE_SERVER}/get-survey-history/v1"
    # Endpoint takes one parameter, study_id, returns complex json of the edit history all surveys
    # on the study, including dates and times, and the json representation of the surveys.

# TARGET_ENDPOINT_URL = f"{MY_BEIWE_SERVER}/get-participant-upload-history/v1"
    # Endpoint takes one required parameter, participant_id, which must match a participant id in a
    # study the user has access to.
    # Returns a json list of dictionaries, containing the file size in bytes, timestamp of the
    # upload, and name of the file. The name can be parsed to identify the data stream, and contains
    # a unix timestamp of the creation time of that file on the device.
    # Accepts the omit_keys parameter.
    # Upload tracking has been in place for the majority of Beiwe's existence, it is pretty reliable.
    # WARNING: this endpoint can return a very large amount of data, and it may be VERY slow.

# TARGET_ENDPOINT_URL = f"{MY_BEIWE_SERVER}/get-participant-heartbeat-history/v1"
    # Endpoint takes one required parameter, participant_id, which must match a participant id in a
    # study the user has access to.
    # Returns a json list of timestamps of the "heartbeats" that a participant's device has sent to
    # the server. These events are sent periodically on a short interval.  (Data may be slightly
    # chaotic for numerous reasons.)
    # Heartbeats were introduced in Beiwe version 2.5.0 for iOS, and 3.6.0 for Android.

# TARGET_ENDPOINT_URL = f"{MY_BEIWE_SERVER}/get-participant-version-history/v1"
    # Endpoint takes one required parameter, participant_id, which must match a participant id in a
    # study the user has access to.  Endpoint also accepts the omit_keys parameter.
    # returns a json list of dictionaries, or a json list of lists, containing the app_version_cod,
    # app_version_name, and os_version.
    # Version history was introduced in early 2024.

# TARGET_ENDPOINT_URL = f"{MY_BEIWE_SERVER}/get-participant-table-data/v1"
    # Endpoint takes two parameters, study_id and data_format.
    # data_format is required and must contain one of "json", "json_table", "csv".
    # Returns a json list of dictionaries, json list of lists, or csv data containing an extended
    # version of the data on the main participant table on the view study page for a study.
    # The complete list of datapoints returned is: Created On, Patient ID, Status, OS Type, Last
    # Upload, Last Survey Download, Last Registration, Last Set Password, Last Push Token Update,
    # Last Device Settings Update, Last OS Version, App Version Code, App Version Name, and Last
    # Heartbeat.

# TARGET_ENDPOINT_URL = f"{MY_BEIWE_SERVER}/get-summary-statistics/v1"
    # Endoint takes many parameters, and has one required parameter, study_id.
    # This endpoint is identical to the Tableau API endpoint. It returns a json list of
    # dictionaries, based on the query parameters.  The query parameters are:
    # `end_date`, a dat of the form YYYY-MM-DD that specifies the last date to include in the search.
    # `start_date`, a date of the form YYYY-MM-DD that specifies the first date to include in the search.
    # `fields`, a comma separated list that of all specific summary statistic fields to return.
    #           Providing no value for fields will return all fields.
    # `limit`, an integer that specifies the maximum number of data points to return.
    # `ordered_by`, a field name that specifies the parameter to sort the output by.
    # `order_direction`, either "ascending" or "descending", specifies the order to sort in.

# TARGET_ENDPOINT_URL = f"{MY_BEIWE_SERVER}/get-participant-device-status-history/v1"
    # Endpoint takes one required parameter, participant_id, which must match a participant id in a
    # study the user has access to.
    # Returns a json list of dictionaries, including complex json of any device status history.
    # Device status history is a feature that must be enabled on a per participant feature by a
    # site admin on the participant experiments page. It is not enabled by default.
    
    # The exact format of the data _differs based on app version_, and was introduced during the
    # latter half of 2023. It is subject to change without warning based on the app version. Some of
    # this data is intended for developer-level debugging and troubleshooting, the exact details of
    # which data points simply require knowledge of the app's internal workings.
    
    # WARNING: this endpoint can return a very large amount of data.

# TARGET_ENDPOINT_URL = f"{MY_BEIWE_SERVER}/get-participant-notification-history/v1"
    # Endpoint takes one required parameter, participant_id, which must match a participant id in a
    # study the user has access to.
    
    # This endpoint takes exactly one optional parameter, `utc`.  If this parameter is present with
    # any value the timestamps will be returned in the UTC timezone, in the usual shorthand
    # indicator of a Z.  If this parameter is not present the timestamps will be returned in the
    # timezone offset of the study that the participant is enrolled in.  (Note that the backend's
    # push notification system is aware of the device's timezone and sends out its notifications
    # accordingly; participants receive notificationss on individualized schedules.)
    
    # Returns a json dictionary.  The keys of the dictionary are strings of the IDs of the survey
    # that the notification was sent for.  These map to a list of dictionaries of data for every
    # notification sent to a participant.  The data has the following keys:
    #   timestamp - The time the notification was sent.
    #   type - The type of schedule that the notification event was based on.
    #   scheduled_time - The scheduled time for this notification - note that sometimes this value
    #         can be well in the past compared to the sent timestamp.
    #   confirmed_received - The time at which the server received a confirmation from the
    #         participant's device that the notification was received.  (This field is only
    #         populated on devices running the iOS app with a version of greater than 2.5.0.)
    #         If there is no value this field will contain a value of False.
    #   uuid - The unicque identifier of the original schedule that this notification was based on.
    #         Note that multiple notifications can be sent for the same schedule.
    #   resend - A boolean value indicating whether this notification was a resend of a previous
    #         notification, for which it should have a matching uuid.  This field has the same OS
    #         and version constraint as confirmed_received.
    #   push_rejected - Notificattions can fail on the server backend for a variety of reasons, tor
    #         example if a particpiant uninstalls the app. This field contains a boolean value of
    #         True if such a failure occurred, otherwise False.

# make a post request to the get-participant-upload-history/v1 endpoint, including the api key,
# secret key, and participant_id as post parameters.
t_start = datetime.now()
print("Starting request at", t_start, flush=True)
response = requests.post(
    TARGET_ENDPOINT_URL,
    
    # refine your parameters here
    data={
        # "access_key": "your key part one",
        # "secret_key": "your key part two",
        
        # several endpoints take a participant id, 
        # "participant_id": "some participant id",
        
        # `omit_keys` is an option on some endpoints, it causes the return data to be potentially
        # much smaller and faster. Format of data will replace dictions with lists of values, order
        # will be retained. It takes a string, "true" or "false".
        # "omit_keys": "true",
        
        # etc.
    },
    allow_redirects=False,
)
t_end = datetime.now()
print("Request completed at", t_end.isoformat(), "duration:", (t_end - t_start).total_seconds(), "seconds")

status_code = response.status_code
raw_output = response.content

# the rest is just some sanity checking to make sure the your request worked and give you basic
# feedback.
print("http status code:", response.status_code)

assert status_code != 400, \
    "400 usually means you are missing a required parameter, or something critical isn't passing some checks.\n" \
    "Check your access key and secret key, if there is a study id make sure it is 24 characters long."

assert status_code != 403, \
    "Permissions Error, you are not authenticated to view data on this study."

assert status_code != 404, \
    "404 means that the entity you have specified does not exist. Check details like study_id, patient_id, etc."

assert response.status_code != 301, \
    "Encountered HTTP redirect, you may have forgotten the s in https. first 100 bytes of response:\n" \
    f"{raw_output[:100]}"

assert response.content != b"", "No data was returned by the server..."

print("Testing whether it is valid json...")
try:
    json_response = orjson.loads(response.content)
    print("JSON successfully loadded into variable `json_response`")
except orjson.JSONDecodeError:
    print("Not valid JSON - which may or may not be an issue! Here is the raw output of the first 100 bytes:")
    print(raw_output[:100])
    json_response = None


# Usually you will want to interact directly with the `json_response` variable.
