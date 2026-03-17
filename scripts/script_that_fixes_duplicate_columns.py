from __future__ import annotations

from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import Generator

from pyzstd import decompress

from constants.common_constants import UTC
from constants.data_stream_constants import (ACCELEROMETER, BLUETOOTH, CALL_LOG, DEVICEMOTION, GPS,
    GYRO, MAGNETOMETER, POWER_STATE, PROXIMITY, REACHABILITY, SURVEY_TIMINGS, TEXTS_LOG, WIFI)
from database.models import ChunkRegistry, Participant, Study
from libs.s3 import NoSuchKeyException, s3_retrieve_no_decompress, s3_upload
from libs.utils.dev_utils import Timer


raise Exception(
    """This script is only meant to handle an issue that occurred on Onnela Lab Servers"""
)


THREAD_POOL_SIZE = 10
STREAMS = [
    ACCELEROMETER,
    BLUETOOTH,
    CALL_LOG,
    DEVICEMOTION,
    GPS,
    GYRO,
    MAGNETOMETER,
    POWER_STATE,
    PROXIMITY,
    REACHABILITY,
    SURVEY_TIMINGS,
    TEXTS_LOG,
    WIFI,
]


# This script has been added to this repo to repair data that should only affect original Onnela Lab
# data on Onnela Lab servers. A bug existed for a brief period, a period hovering around March 22-24
# in 2024. Affected data would get an extra copy of the two datetime columns inserted if it was
# _re_processed during this period.  This script repairs that data without requiring a reprocessing
# of all data.


# these are the progression of our tested ranges

# TIME_BIN_MID = datetime(2024, 2, 1, tzinfo=UTC)  # initial guess
# DURATION = timedelta(days=60)                    # plus-minus 60 days
# TIME_BIN_START = TIME_BIN_MID - DURATION         # December 3
# TIME_BIN_END = TIME_BIN_MID + DURATION           # April 1

# based on the output of a variant of the script from teh above guess
# TIME_BIN_START = datetime(2024, 3, 22, 14, 0, 0, tzinfo=UTC) - timedelta(days=3)
# TIME_BIN_END = datetime(2024, 3, 25, 2, 0, 0, tzinfo=UTC) + timedelta(days=3)

# refinement from production
# TIME_BIN_START = datetime(2024, 3, 22, 14, 0, 0, tzinfo=UTC) - timedelta(days=7)
# TIME_BIN_END = datetime(2024, 3, 22, 14, 0, 0, tzinfo=UTC) - timedelta(days=3)

# more refinement
# TIME_BIN_START = datetime(2024, 3, 22, 14, 0, 0, tzinfo=UTC) - timedelta(days=12)
# TIME_BIN_END = datetime(2024, 3, 22, 14, 0, 0, tzinfo=UTC) - timedelta(days=7)


# Date range from the run on production - this is thought to be 99%+ of the affected files. There
# was one participant affected data back to March 4th, there could be others but it is unlekely
# because the bad state only existed for a short period and the participant had to have been unable
# to upload data from weeks ago for weeks after collecting it.
TIME_BIN_START = datetime(2024, 3, 11, 0, 0, 0, tzinfo=UTC)
TIME_BIN_END = datetime(2024, 3, 26, 0, 0, 0, tzinfo=UTC)


def drain_list_or_dict(l: list[tuple[bytes, str]]) -> Generator[tuple[bytes, str], None, None]:
    while l:
        yield l.pop(-1)


def fix_csv_text(csv_text: bytes) -> bytes:
    """ Fix corrupted CSV: remove extra date cols taking the first 2 and the last N-minus-2
    columns, and then running a deduplicate and sort by timestamp. """
    
    lines = csv_text.splitlines()
    if not lines:
        return b""
    
    if len(lines) == 1:
        return lines[0] + b"\n"
    
    # Header
    header = lines[0]
    expected_commas = header.count(b",")
    expected_cols = header.count(b",") + 1
    
    cleaned_lines = []
    seen_lines = set()
    seen_lines_add = seen_lines.add  # macro-optimization
    cleaned_lines_append = cleaned_lines.append
    for line in lines[1:]:
        
        if not line.strip():  # case: empty line
            continue
        
        if line.count(b",") <= expected_commas:  # less than.... is a bug we haven't seen so aren't fixing.
            seen_lines_add(line)
            cleaned_lines_append(line)
            continue
        
        half_1 = line.split(b",", 2)[:2]
        half_2 = line.rsplit(b",", expected_cols - 2)[1:]
        cleaned = b",".join(half_1 + half_2)
        if cleaned in seen_lines:
            continue
        cleaned_lines_append(cleaned)
        seen_lines_add(cleaned)
    
    cleaned_lines.sort(key=lambda x: int(x[:10]))
    ret = header + b"\n" + b"\n".join(cleaned_lines)
    cleaned_lines.clear()
    seen_lines.clear()
    del cleaned_lines, seen_lines
    return ret


def paginate_chunks(p: Participant) -> Generator[list[str], None, None]:
    """ This is where the filtering lives. It is time and data types  """
    
    ret = []
    q = p.chunk_registries.filter(
        data_type__in=STREAMS,
        time_bin__gte=TIME_BIN_START,
        time_bin__lte=TIME_BIN_END,
    ).vlist("chunk_path").iterator()
    
    for chunk_path in q:
        ret.append(chunk_path)
        if len(ret) >= 100:
            yield ret
            ret.clear()
            del ret
            ret = []
    
    if ret:
        yield ret
        ret.clear()
        del ret


# threaded dispatch .... crud
def batch_download(args: tuple[str, Study, bool]) -> tuple[bytes, str]:
    try:
        return s3_retrieve_no_decompress(*args), args[0]
    except NoSuchKeyException as e:
        print(f"uhoh chunk not found: `{args[0]}`")
        return b"", args[0]


def push_fixed_data(path: str, data: bytes, study: Study):
    """ doesn't need to be batched """
    ChunkRegistry.objects.filter(chunk_path=path).update(file_size=len(data))
    s3_upload(path, data, study, raw_path=True)



def process_participant(p: Participant, study: Study):
    """
    
    Processes participants within the time range for the affected data types
    Can be used to run on one particpiant, edit as appropriate.
    
    """
    
    
    print("\nStarting", p.patient_id, "\n")
    
    for i, list_of_paths in enumerate(paginate_chunks(p)):
        
        print(f"downloading {len(list_of_paths)} ({i}) chunks for participant {p.patient_id}...")
        
        args = [(path, study, True) for path in list_of_paths]  # True
        pool = ThreadPool(THREAD_POOL_SIZE)
        with Timer() as t:
            data_and_path = list(pool.imap_unordered(batch_download, args))
        print(f"downloaded {sum(len(a) for a, b in data_and_path)/1024/1024:,.2f} MB in {t.fseconds} seconds")
        
        pool.close()
        pool.join()
        pool.terminate()
        
        for old_data, path in drain_list_or_dict(data_and_path):
            old_data = decompress(old_data)
            new_data = fix_csv_text(old_data)
            
            if new_data != old_data:
                
                print(f"{path} mutated...")
                print(f"uploading fixed chunk of size {len(old_data)} -> {len(new_data)} to {path}...")
                print(f"old line count: {old_data.count(b'\n') if old_data else 0}, new line count: {new_data.count(b'\n') if new_data else 0}")
                
                # print("\n\n")
                # print(old_data.decode().splitlines())
                # print("\n\n")
                # print(new_data.decode().splitlines())
                
                push_fixed_data(path, new_data, study)
                
        print()



def main():
    # current configuration is to run this over active studies
    for s in Study.objects.filter(deleted=False):
        print("starting study " + s.name)
        
        for p in s.participants.filter(deleted=False):
            process_participant(p, s)
