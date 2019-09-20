# This script is tested to have identical output and be compatible with Python 2.7 and 3.6.
# (It really should be compatible with any version of Python 3.)
import base64
import csv
import hashlib
import sys
from datetime import datetime
from os.path import sep as SYSTEM_FOLDER_SEPARATOR
from sys import argv

# This script has 2 dependencies: pytz and python-dateutil
# pytz is effectively part of the python std library, but it needs to be updated more frequently
# than python itself is updated, and having it as a non-std library allows these changes to be
# centralized.
# python-dateutil is a library that contains a fuzzy date format parser.
try:
    import pytz
    from dateutil.parser import parse
except ImportError:
    print("This script requires the 'pytz' and 'python-dateutil' library be installed. "
          "These libraries are required to handle timezone conversions correctly.")
    print("Installing pytz should be as simple as running 'pip install pytz python-dateutil'.")
    exit(1)

IS_PYTHON_2 = True if sys.version[0] == '2' else False

OUTPUT_COLUMNS = [
    "timestamp",
    "UTC time",
    "hashed phone number",
    "sent vs received",
    "message length",
    "time sent",
]

NECESSARY_COLUMNS = [
    "Message Date",         # timestamp
    "Type",                 # "Incoming" or "Outgoing"
    "Text",                 # message content
    "Sender ID",            # Usually a phone number, might be a different value, gets hashed
]

TZ_HELP_STRING = "--tz-help"
INPUT_CSV_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
OUTPUT_CSV_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
START_END_DATE_FORMAT = "%Y-%m-%d"
PYTHON_FILE_NAME = __file__.split(SYSTEM_FOLDER_SEPARATOR)[-1] if SYSTEM_FOLDER_SEPARATOR in __file__ else __file__

TIME_FORMAT_ERROR_WARNING = "WARNING: this file contains date strings with an ambiguous formatting."
TIME_FORMAT_ERROR_WARNING_FLAG = 0
HASH_CACHE = {}


###############
### Helpers ###
###############

def add_tz_to_naive_datetime(dt):
    # is_dst=False is the default, sticking it here for completeness
    return tz.localize(dt, is_dst=False)


def input_csv_datetime_string_to_tz_aware_datetime(dt_string):
    try:
        dt = datetime.strptime(dt_string, INPUT_CSV_DATE_FORMAT)
    except ValueError:
        global TIME_FORMAT_ERROR_WARNING_FLAG
        if TIME_FORMAT_ERROR_WARNING_FLAG < 1:
            print(TIME_FORMAT_ERROR_WARNING)
            TIME_FORMAT_ERROR_WARNING_FLAG += 1
        dt = parse(dt_string)
    return add_tz_to_naive_datetime(dt)


def dt_to_utc_timestamp(dt):
    # this appears to be the most compatible way to get the unix timestamp.
    return dt.strftime("%s")


def dt_to_output_format(dt):
    # convert to the datetime string format from beiwe backend
    return dt.strftime(OUTPUT_CSV_DATE_FORMAT)


def hash_contact_id(contact_id):
    """ The Android app mechanism for hashing contacts is to use pbkdf2 on the last 10 digits of
    the contact's phone number.  To imitate this exactly requires more information than is
    available in the script (we would need an iteration count and a salt).
    Instead all we do here is a single, standard SHA256 round.
    (contact_id in a csv is usually the digits of a phone number, but this is not always the case.
    """
    # Python 2 vs 3, encode/decode issues. Normalize encoding of the base64 output string to a
    # string in all cases (unicode object in python 2, str in python 3).

    # hashing is slow, but we can cache to get a huge speedup.
    if contact_id in HASH_CACHE:
        return HASH_CACHE[contact_id]

    sha256 = hashlib.sha256()

    # iteration count of 512
    def update_many_times():
        for _ in range(10000):
            sha256.update(sha256.digest())

    # the python 2 and 3 code has been tested, they result in the same output.
    if IS_PYTHON_2:
        sha256.update(contact_id)
        update_many_times()
        digest = sha256.digest()
        ret = base64.urlsafe_b64encode(digest)
    else:
        sha256.update(contact_id.encode())    # unicode -> bytes
        update_many_times()
        digest = sha256.digest()
        ret = base64.urlsafe_b64encode(digest)
        ret = ret.decode()                    # bytes -> unicode

    # clear any new lines, cache result
    ret = ret.replace("\n", "")
    HASH_CACHE[contact_id] = ret
    return ret


def consistent_character_length(text):
    # python 2 and 3 have different string formats, we want to use unicode encoding because
    # the length should be the number of characters, not the number of 7-bit ascii bytes.
    if IS_PYTHON_2:
        return len(text.decode("utf-8"))
    else:
        return len(text)


def determine_within_end_start(dt):
    """ handles the logic for various cases of whether start and end date parameters were provided """
    if END_DATE and START_DATE:
        return START_DATE < dt < END_DATE
    elif END_DATE:
        pass # case cannot occur, only start date on its own
    elif START_DATE:
        return START_DATE < dt
    else:
        return True


#############################
### Arg Parsing and Setup ###
#############################
# argv[0] = name of this file
# argv[1] = path of csv file to operate on
# argv[2] = timezone to assume when extracting data
# argv[3] = optional: date to start getting data
# argv[4] = optional: date to stop getting data
# if the user has provided the timezone help argument anywhere print all timezones

if TZ_HELP_STRING in argv:
    print("All timezone options (%s):" % len(pytz.all_timezones))
    for t in pytz.all_timezones:
        print("\t", t)
    exit(0)

if len(argv) in [1, 2]:
    print("Usage: %s csv_file_to_parse timezone" % PYTHON_FILE_NAME)
    print()
    print("First parameter: a csv file.")
    print("Second parameter: a string indicating the timezone to use.")
    print("Third parameter (optional): a start date of events to include, of the form YYYY-MM-DD.")
    print("Fourth parameter (optional): an end date of events to include, of the form YYYY-MM-DD.")
    print()
    print("(Try '%s %s' for a listing of available timezones)." % (PYTHON_FILE_NAME, TZ_HELP_STRING))
    exit(1)

tz = argv[2]
try:
    tz = pytz.timezone(tz)
except pytz.UnknownTimeZoneError:
    print("Unrecognized timezone:", tz)
    print("(Try '%s %s' for a listing of available timezones)." % (PYTHON_FILE_NAME, TZ_HELP_STRING))
    exit(1)

INPUT_FILE_NAME = argv[1]

# attempt to get a nice-ish output file name right next to the input file.
if INPUT_FILE_NAME.endswith(".csv"):
    OUTPUT_FILE_NAME = INPUT_FILE_NAME[:-4] + ".out.csv"
else:
    OUTPUT_FILE_NAME = INPUT_FILE_NAME + ".out.csv"

# start.
if len(argv) > 3:
    try:
        START_DATE = add_tz_to_naive_datetime(datetime.strptime(argv[3], START_END_DATE_FORMAT))
    except ValueError as e:
        print("The date provided for start date is either not of the form YYYY-MM-DD, or is an invalid date.")
        exit(1)
else:
    START_DATE = None

# end.
if len(argv) > 4:
    try:
        END_DATE = add_tz_to_naive_datetime(datetime.strptime(argv[4], START_END_DATE_FORMAT))
    except ValueError as e:
        print("The date provided for end date is either not of the form YYYY-MM-DD, or is an invalid date.")
        exit(1)
else:
    END_DATE = None

if START_DATE and END_DATE:
    if START_DATE > END_DATE:
        print("\nError: start date must come before end date.")
        exit(1)

# This method of consuming the input file should handle all newline cases correctly.
with open(INPUT_FILE_NAME) as f:
    csv_reader = csv.DictReader(f.read().splitlines())

# throw any errors about missing, necessary
for fieldname in NECESSARY_COLUMNS:
    assert fieldname in csv_reader.fieldnames, "This file is missing the '%s' column." % fieldname

############
### Main ###
############

def extract_data():
    output_rows = []
    for input_row in csv_reader:
        output_row = {}

        # First get the datetime objects we will need
        dt = input_csv_datetime_string_to_tz_aware_datetime(input_row["Message Date"])

        # determine if row is within date range
        if not determine_within_end_start(dt):
            continue

        unix_timestamp = dt_to_utc_timestamp(dt)

        # text csv has 3 timestamps, but they all are from the same source
        output_row["timestamp"] = unix_timestamp
        output_row["time sent"] = unix_timestamp
        output_row["UTC time"] = dt_to_output_format(dt)

        # get a hashed id of the message sender (always the same for any given user)
        output_row["hashed phone number"] = hash_contact_id(input_row["Sender ID"])

        # length row is very simple.
        output_row["message length"] = consistent_character_length(input_row["Text"])

        # populate sent vs received with the expected string.
        # (doing a case insensitive compare for paranoid safety.)
        message_type = input_row["Type"].lower()
        output_row["sent vs received"] = "received SMS" if message_type == "incoming" else "sent SMS"

        # assemble the data into a correctly ordered list
        output_rows.append([output_row[column] for column in OUTPUT_COLUMNS])

    # sort by time (integer value of first column, which is the unix timestamp)
    output_rows.sort(key=lambda x: int(x[0]))
    return output_rows


def write_data(output_rows):
    # write the extracted data to a csv file (default csv dialect is excel) next to the input file
    with open(OUTPUT_FILE_NAME, "w") as out_file:
        csv_writer = csv.writer(out_file)
        csv_writer.writerow(OUTPUT_COLUMNS)
        for output_row in output_rows:
            csv_writer.writerow(output_row)
    print("Data conversion finished, output file is", OUTPUT_FILE_NAME)


write_data(extract_data())
