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
        data_type = UPLOAD_FILE_TYPE_MAPPING.get(file_piece)
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


def ensure_sorted_by_timestamp(rows_of_lists_of_bytes: list[list[bytes]]):
    """
    Sorts the list with a bunch of error handling to provide the correct error message, and I did
    a bunch of profiling to make sure this is fast.
    
    First value in every row should be a byte-string in the form of digits, an integer-like string.
    Happens to support str and bytes.
    
                                 Should raise ValueErrors.
    
    Details
    1) We need to detect bad sort inputs, including the case where a string like this:
          b'1771452000058,2026-02-18T22:00:00.058,1,0.4707878530025482,..."
          
      gets passed in as a whole single row. (this is a historical failure mode where the row
      passes through validly and is sorted according to the first character)
    
    2) The use of "item := row[0]" is very slightly faster than two x[0]
    
    3) Assigning local variables like `i = int` and `isdigit = bytes.isdigit` is slower than this
       normal construction.  (cpython 3.12)
    
    4) using `isinstance(item, bytes)` is slower than probably_an_bytes.isdigit() (strings will
       pass and work, that's fine)
    
    5) The (profiled) additional cost of this sort-key function is roughly 20%, old key was
          rows_of_lists_of_bytes.sort(key=lambda row: int(row[0]))
    
    6) The `else None` is specific, None is not comparable via < with another None, which can
       occur if TWO values are non-digit strings. (there's a test)
    """
    
    try:
        # There is a type warning beecause `else None` is not an int - the type mixing is intentional
        rows_of_lists_of_bytes.sort(
            key=lambda row: int(bytes_item) if (bytes_item := row[0]).isdigit() else None  # type: ignore
        )
    except (AttributeError, TypeError) as e:
        # this error is real hard to read, so let's make our lives easier:
        for i, row in enumerate(rows_of_lists_of_bytes):
            if not isinstance(row, (list, tuple)):
                raise ValueError(f"invalid sort value in row {i}, `{row}`") from e
            if not row:
                raise ValueError(f"empty row at index {i}") from e
            if not row[0]:
                raise ValueError(f"falsey timestamp value in row {i}: `{row}`, {type(row[0])}") from e
            if not isinstance(row[0], bytes):
                raise ValueError(f"invalid sort value in row {i}, `{row}`, encountered {type(row[0])}") from e
            if not row[0].isdigit():
                raise ValueError(f"invalid sort value in row {i}, `{row}`") from e
        raise  # raise other unexpected errors as normal


def convert_unix_to_human_readable_timestamps(header: bytes, rows: list[list[bytes]]) -> bytes:
    """ Adds a new column to the end which is the unix time represented in
    a human readable time format.  Returns an appropriately modified header. """
    for row in rows:
        unix_millisecond = int(row[0])  # line can fail due to wrong os on the FileToProcess object.
        time_string = unix_time_to_string(unix_millisecond // 1000)
        # this line 0-pads millisecond values that have leading 0s.
        time_string += b".%03d" % (unix_millisecond % 1000)
        row.insert(1, time_string)
    
    split_header: list[bytes] = header.split(b",")
    split_header.insert(1, b"UTC time")
    return b",".join(split_header)


def binify_from_timecode(unix_ish_time_code_string: bytes | str) -> int:
    """ Takes a unix-ish time code (accepts unix millisecond), and returns an
        integer value of the bin it should go in. """
    # integer divide by the 3600 (an hour of seconds) to be used as the key in binified data
    # which acts to separate data into hourly chunks
    return clean_java_timecode(unix_ish_time_code_string) // CHUNK_TIMESLICE_QUANTUM


def clean_java_timecode(unix_ish_time_code_string: bytes | str) -> int:
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
