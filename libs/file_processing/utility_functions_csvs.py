import re
from datetime import datetime

from constants.common_constants import API_TIME_FORMAT, UTC


def insert_timestamp_single_row_csv(header: bytes, rows_list: list[list], time_stamp: bytes) -> bytes:
    """ Inserts the timestamp field into the header of a csv, inserts the timestamp
        value provided into the first column.  Returns the new header string."""
    header_list = header.split(b",")
    header_list.insert(0, b"timestamp")
    rows_list[0].insert(0, time_stamp)
    return b",".join(header_list)


# this is a regex that matches the format of the timestamp string we expect to see in the data.
# (it is very fast)
is_it_a_date_string = re.compile(r"^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d$")


# this is the normal version of the function to revert to after the run to fix existing data finishes.
def csv_to_list_of_list_of_bytes(file_bytes: bytes) -> tuple[bytes, list[list[bytes]]]:
    lines = file_bytes.splitlines()
    return lines.pop(0), [l.split() for l in lines]


# keeping this around in case anyone encounters the bug in issue
# https://github.com/onnela-lab/beiwe-backend/issues/373
# def csv_to_list_of_list_of_bytes(file_bytes: bytes) -> Tuple[bytes, List[List[bytes]]]:
#     # due to a bug on servers roughly 4 days in march 2024, we have to apply some fix to data
#     # that has already been processed.  This only needs to happen once per affected file.
#     lines = file_bytes.splitlines()
#     header = lines.pop(0)
    
#     ret_lines = []
#     for line in lines:
#         good_components = []
#         line_components = line.split(b",")
#         # line[0] is a unix milliseconds timestamp
#         # line[1] is a human readable timestamp in the format of API_TIME_FORMAT
#         good_components.append(line_components.pop(0))
        
#         should_be_human_timestamp = line_components.pop(0)
#         if not is_it_a_date_string.match(should_be_human_timestamp.decode()):
#             error = line + b" - " + should_be_human_timestamp
#             raise Exception(f"encountered a non-timestamp value: {error.decode()}")
        
#         good_components.append(should_be_human_timestamp)
        
#         # the rest should be columns of data, but with duplicates of the human readable timestamps.
#         # for each value, if it isn't such a timestamp, add it to the list of good components.
#         # there is no situation where there should be a perfectly formatted timestamp with two commas
#         # on either side, so we can always remove it.
#         # we are only running this on ACCELEROMETER, BLUETOOTH, CALL_LOG, DEVICEMOTION, GPS, GYRO,
#         # MAGNETOMETER, POWER_STATE, PROXIMITY, REACHABILITY, TEXTS_LOG, and WIFI
#         for value in line_components:
#             if not is_it_a_date_string.match(value.decode()):
#                 good_components.append(value)
        
#         ret_lines.append(good_components)
    
#     return header, ret_lines


# def csv_to_list(file_contents: bytes) -> Tuple[bytes, List[bytes]]:
#     """ Grab a list elements from of every line in the csv, strips off trailing whitespace. dumps
#     them into a new list (of lists), and returns the header line along with the list of rows. """
    
#     # This code is more memory efficient than fast by using a generator
#     # Note that almost all of the time is spent in the per-row for-loop
    
#     # case: the file coming in is just a single line, e.g. the header.
#     # Need to provide the header and an empty iterator.
#     if b"\n" not in file_contents:
#         return file_contents, []
    
#     lines = file_contents.splitlines()
#     return lines.pop(0), list(line.split(b",") for line in lines)

# We used to have a generator version of this function that nominally has better memory usage, but
# it was slower than just doing the splitlines, and caused problems with fixes after the last
# section of the file processing refactor.
#     line_iterator = isplit(file_contents)
#     header = b",".join(next(line_iterator))
#     header2 = file_contents[:file_contents.find(b"\n")]
#     assert header2 == header, f"\n{header}\n{header2}"
#     return header, line_iterator
# def isplit(source: bytes) -> Generator[List[bytes], None, None]:
#     """ Generator version of str.split()/bytes.split() """
#     # version using str.find(), less overhead than re.finditer()
#     start = 0
#     while True:
#         # find first split
#         idx = source.find(b"\n", start)
#         if idx == -1:
#             yield source[start:].split(b",")
#             return
#         yield source[start:idx].split(b",")
#         start = idx + 1
# def isplit2(source: bytes) -> Generator[bytes, None, None]:
#     """ This is actually faster, almost as fast as the splitlines method, but it returns items in reverse order... """
#     lines = source.splitlines()
#     del source
#     yield lines.pop(0)
#     while lines:
#         yield lines.pop(-1).split(b",")

def construct_csv_as_bytes(header: bytes, rows_list: list[list[bytes]]) -> bytes:
    """ Takes a header list and a bytes-list and returns a single string of a csv. Very performant."""
    
    def deduplicate(seq: list[bytes]):
        # todo on python 3.11 - this pattern with the cached variable name is probably slower
        # highly optimized order preserving deduplication function.
        seen = set()
        seen_add = seen.add
        # list comprehension is slightly slower, tuple() is faster for smaller counts, list()
        #  is very slightly faster on large counts.  tuple() *should* have lower memory overhead?
        return tuple(x for x in seq if not (x in seen or seen_add(x)))
    
    # this comprehension is always fastest, there is no advantage to inlining the creation of rows
    rows = [b",".join(row_items) for row_items in rows_list]
    # we need to ensure no duplicates
    rows = deduplicate(rows)
    
    # doing this as a repeated += add is better memory use, but at least 100x slower.
    return header + b"\n" + b"\n".join(rows)


def unix_time_to_string(unix_time: int) -> bytes:
    return datetime.fromtimestamp(unix_time, UTC).strftime(API_TIME_FORMAT).encode()
