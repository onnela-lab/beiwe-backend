from typing import List

from constants import common_constants
from constants.common_constants import EARLIEST_POSSIBLE_DATA_TIMESTAMP
from constants.data_processing_constants import CHUNK_TIMESLICE_QUANTUM
from constants.data_stream_constants import IDENTIFIERS, IOS_LOG_FILE, UPLOAD_FILE_TYPE_MAPPING
from libs.file_processing.utility_functions_csvs import unix_time_to_string


class BadTimecodeError(Exception): pass


def normalize_s3_file_path(s3_file_path: str) -> str:
    if "duplicate" in s3_file_path:
        # duplicate files are named blahblah/datastream/unixtime.csv-duplicate-[rando-string]
        return s3_file_path.split("-duplicate")[0]
    else:
        return s3_file_path


def s3_file_path_to_data_type(file_path: str):
    # Look through each folder name in file_path to see if it corresponds to a data type. Due to
    # a dumb mistake ages ago the identifiers file has an underscore where it should have a
    # slash, and we have to handle that case.  Also, it looks like we are hitting that case with
    # the identifiers file separately but without any slashes in it, sooooo we need to for-else.
    file_path = normalize_s3_file_path(file_path)
    for file_piece in file_path.split('/'):
        data_type = UPLOAD_FILE_TYPE_MAPPING.get(file_piece, None)
        if data_type and "identifiers" in data_type:
            return IDENTIFIERS
        if data_type:
            return data_type
    else:
        if "identifiers" in file_path:
            return IDENTIFIERS
        if "ios/log" in file_path:
            return IOS_LOG_FILE
    # If no data type has been selected; i.e. if none of the data types are present in file_path,
    # raise an error
    raise Exception(f"data type unknown: {file_path}")


def resolve_survey_id_from_file_name(name: str) -> str:
    name = normalize_s3_file_path(name)
    return name.rsplit("/", 2)[1]


def ensure_sorted_by_timestamp(l: list):
    """ In-place sorting is fast, but we (may) need to purge rows that are broken.
        Purging broken rows is A) exceedingly uncommon, B) potentially VERY slow."""
    try:
        # first value should be a timestamp integer-like string, we get a ValueError if it isn't.
        l.sort(key=lambda x: int(x[0]))
    except ValueError:
        # get bad rows, pop them off, sort again
        bad_rows = []
        for i, row in enumerate(l, 0):  # enumerate in this context needs to start at 0
            try:
                int(row[0])
            except ValueError:
                bad_rows.append(i)
        for bad_row in reversed(bad_rows):
            l.pop(bad_row)  # SLOW.
        l.sort(key=lambda x: int(x[0]))


def convert_unix_to_human_readable_timestamps(header: bytes, rows: List[List[bytes]]) -> List[bytes]:
    """ Adds a new column to the end which is the unix time represented in
    a human readable time format.  Returns an appropriately modified header. """
    for row in rows:
        unix_millisecond = int(row[0])  # line can fail due to wrong os on the FileToProcess object.
        time_string = unix_time_to_string(unix_millisecond // 1000)
        # this line 0-pads millisecond values that have leading 0s.
        time_string += b".%03d" % (unix_millisecond % 1000)
        row.insert(1, time_string)
    header: List[bytes] = header.split(b",")
    header.insert(1, b"UTC time")
    return b",".join(header)


def binify_from_timecode(unix_ish_time_code_string: bytes) -> int:
    """ Takes a unix-ish time code (accepts unix millisecond), and returns an
        integer value of the bin it should go in. """
    # integer divide by the 3600 (an hour of seconds) to be used as the key in binified data
    # which acts to separate data into hourly chunks
    return clean_java_timecode(unix_ish_time_code_string) // CHUNK_TIMESLICE_QUANTUM


def clean_java_timecode(unix_ish_time_code_string: bytes) -> int:
    try:
        timestamp = int(unix_ish_time_code_string[:10])
    except ValueError as e:
        # we need a custom error type to handle this error case
        raise BadTimecodeError(str(e))
    
    if timestamp < EARLIEST_POSSIBLE_DATA_TIMESTAMP:
        raise BadTimecodeError("data too early")
    
    # FIXME: refactor data processing and get rid of this runtime hack
    if common_constants.LATEST_POSSIBLE_DATA_TIMESTAMP < timestamp:
        raise BadTimecodeError("data too late")
    
    return timestamp
