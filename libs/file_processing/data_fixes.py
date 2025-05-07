from libs.file_processing.utility_functions_csvs import insert_timestamp_single_row_csv
from libs.file_processing.utility_functions_simple import normalize_s3_file_path


def fix_survey_timings(header: bytes, rows_list: list[list[bytes]], file_path: str) -> bytes:
    """ Survey timings need to have a column inserted stating the survey id they come from."""
    survey_id = file_path.rsplit("/", 2)[1].encode()
    for row in rows_list:
        row.insert(2, survey_id)
    header_list = header.split(b",")
    header_list.insert(2, b"survey id")
    return b",".join(header_list)


def fix_call_log_csv(header: bytes, rows_list: list) -> bytes:
    """ The call log has poorly ordered columns, the first column should always be
        the timestamp, it has it in column 3.
        Note: older versions of the app name the timestamp column "date". """
    for row in rows_list:
        row.insert(0, row.pop(2))
    header_list = header.split(b",")
    header_list.insert(0, header_list.pop(2))
    return b",".join(header_list)


def fix_identifier_csv(header: bytes, rows_list: list, file_name: str) -> bytes:
    """ The identifiers file has its timestamp in the file name. """
    file_name = normalize_s3_file_path(file_name)
    time_stamp = file_name.rsplit("_", 1)[-1][:-4].encode() + b"000"
    return insert_timestamp_single_row_csv(header, rows_list, time_stamp)


def fix_wifi_csv(header: bytes, rows_list: list, file_name: str) -> bytes:
    """ Fixing wifi requires inserting the same timestamp on EVERY ROW.
    The wifi file has its timestamp in the filename. """
    file_name = normalize_s3_file_path(file_name)
    time_stamp = file_name.rsplit("/", 1)[-1][:-4].encode()
    
    # the last row is a new line, have to slice.
    for row in rows_list[:-1]:
        row = row.insert(0, time_stamp)
    
    if rows_list:
        # remove last row (encountered an empty wifi log on sunday may 8 2016)
        rows_list.pop(-1)
    
    return b"timestamp," + header


def fix_app_log_file(file_contents: bytes, file_name: str):
    """ The log file is less of a csv than it is a time enumerated list of events, with the time
        code preceding each row.
        We insert a base value, a new row stating that a new log file was created, which allows us
        to guarantee at least one timestamp in the file."""
    file_name = normalize_s3_file_path(file_name)
    time_stamp = file_name.rsplit("/", 1)[-1][:-4].encode()
    rows_list = file_contents.splitlines()
    rows_list[0] = time_stamp + b" New app log file created"
    new_rows = []
    for row in rows_list:
        # row_elements has len 2, element 0 is a java timecode, element 1 is the text of the log.
        row_elements = row.split(b" ", 1)
        try:
            int(row_elements[0])  # grab first element, check if it is a valid int
            new_rows.append(row_elements)
        except ValueError:
            if (b"bluetooth Failure" == row[:17] or
                b"our not-quite-race-condition" == row[:28] or
                b"accelSensorManager" in row[:18] or  # this actually covers 2 cases
                b"a sessionactivity tried to clear the" == row[:36]
            ):
                # Just drop matches to the above lines
                continue
            else:
                # Previously this was a whitelist of broken lines to explicitly insert a timecode
                # on, but now it is generalized:
                # 'new_rows[-1][0]' is the timecode of the previous line in the log.
                new_rows.append((new_rows[-1][0], row))
                continue
    
    return b"timestamp, event\n" + b"\n".join(b",".join(row) for row in new_rows)
