from collections import defaultdict
from datetime import datetime
from itertools import batched
from pprint import pprint

from django.utils import timezone

from constants.common_constants import UTC
from constants.data_stream_constants import (ALL_DATA_STREAMS, DATA_STREAM_TO_S3_FILE_NAME_STRING,
    SURVEY_TIMINGS)
from database.models import FileToProcess, Participant, S3File, Study


def get_studies(*studies: str) -> list[Study]:
    ret = []
    for study_object_id in studies:
        
        try:
            s = Study.objects.get(object_id=study_object_id)
        except Study.DoesNotExist:
            print(f"study `{study_object_id}` does not exist, please provide only valid study ids")
            exit(1)
        
        if s.deleted:
            print(f"study `{study_object_id}` was deleted / retired, plese provide only valid study ids")
            exit(2)
        
        ret.append(s)
    
    return ret


# Just modify this list, add those 24 character study ids of the studies you want to rerun processing on
just_a_list_of_study_ids = [
    "study_id_1",
    "study_id_2",
    "another_study_id",
    "etc",
]

studies = get_studies(*just_a_list_of_study_ids)


####################################################################################################

# remove specific data streams that are just served raw
UPLOAD_FILES_FILTER = list(DATA_STREAM_TO_S3_FILE_NAME_STRING.values())

if "ai_chat_logs" in UPLOAD_FILES_FILTER:
    UPLOAD_FILES_FILTER.remove("ai_chat_logs")
if "ambient_audio" in UPLOAD_FILES_FILTER:
    UPLOAD_FILES_FILTER.remove("ambient_audio")

UPLOAD_FILES_FILTER.remove("surveyAnswers")
UPLOAD_FILES_FILTER.remove("voiceRecording")

# We need the slashes on both sides because of -duplicate-rando
UPLOAD_FILES_FILTER = [f"/{stream}/" for stream in UPLOAD_FILES_FILTER]
ALL_DATA_STREAMS_FILTER = [f"/{stream}/" for stream in ALL_DATA_STREAMS]

# there was a bug fix before which we should not reprocess survey timings
TIMINGS_TOO_EARLY_TO_CLEAR = datetime(2018, 1, 1, tzinfo=UTC)


def main():
    
    print("\n\n\nThis may take a while\n\n\n")
    
    for study in studies:
        for participant in study.participants.filter(deleted=False):
            print("starting participant " + participant.patient_id)
            # first need to delete existing processed survey timings
            participant.chunk_registries.filter(
                data_type=SURVEY_TIMINGS,
                time_bin__lt=TIMINGS_TOO_EARLY_TO_CLEAR,
            ).delete()
            process_participant(participant, study.object_id)
            
            print()
            print("="*80)
            print()


def process_participant(p: Participant, study_obj_id: str):
    print(f"getting file list for participant {p.patient_id}...")
    
    t0 = timezone.now()
    upload_files = list(
        S3File.fltr(path__startswith=f"{study_obj_id}/{p.patient_id}/")
        .order_by("path").values_list("path", "size_uncompressed")
    )
    download_files = list(
        S3File.fltr(path__startswith=f"CHUNKED_DATA/{study_obj_id}/{p.patient_id}/")
        .order_by("path").values_list("path", "size_uncompressed")
    )
    
    print(f"retrieved file list in {(timezone.now() - t0).total_seconds():.2f} seconds")
    print(f"found {len(upload_files)} uploaded files and {len(download_files)} chunked data stream files")
    
    paths = filter_uploads(upload_files)
    initial_len = len(paths)
    summarize_chunked_data(download_files)
    
    upload_files.clear()
    download_files.clear()
    del upload_files, download_files
    
    # remove files already in FileToProcess
    paths = list(set(paths) - set(p.files_to_process.values_list("s3_file_path", flat=True)))
    
    # paths = remove_early_files(paths)  # commented out feature to not process files from before X
    # print(f"removed {initial_len - len(paths)} files from before 2023, {len(paths)} remain")
    
    paths.sort()  # make this ~deterministic
    
    print()
    print(f"removed {initial_len - len(paths)} files that are already queued, {len(paths)} remain")
    print()
    print("creating FileToProcesses...es...")
    
    t1 = timezone.now()
    new_ftps = []
    for i, batch in enumerate(batched(paths, 1000)):
        print(f"building 1000 files ({i})...")
        for path in batch:
            new_ftps.append(generate_file_to_process(path, p))
    
    print("bulk creating...")
    FileToProcess.objects.bulk_create(new_ftps)
    
    print(f"done adding files, took {(timezone.now() - t1).total_seconds():.2f} seconds")


def remove_early_files(paths: list[str]) -> list[str]:
    # removes files from before 2023 - disabled by default
    
    milliseconds = 1672531200 * 1000  # January 1, 2023, unix milliseconds
    
    filtered_paths = []
    for path in paths:
        try:
            unix_time = int(path.rsplit("/", 1)[-1].split(".")[0])
            if unix_time >= milliseconds:  # January 1, 2023 in milliseconds
                filtered_paths.append(path)
        except ValueError:
            print(f"could not parse unix time from path `{path}`, skipping")
            raise
    
    return filtered_paths


def generate_file_to_process(file_path: str, participant: Participant) -> FileToProcess:
    return FileToProcess(
        s3_file_path=FileToProcess.ensure_study_prefix_in_path(file_path, participant.study.object_id),
        participant=participant,
        study=participant.study,
        os_type=participant.os_type,
        app_version=participant.last_version_code or "",
    )


def filter_uploads(q1: list[tuple[str, int]]) -> list[str]:
    paths = []
    upload_info = defaultdict(int)
    for path, size in q1:
        if "//" in path:
            continue
        
        for stream in UPLOAD_FILES_FILTER:
            if stream in path:
                
                if "surveyTimings" in path and path.count("/") != 4:
                    # really old survey timings may not have the correct path structure to reprocess
                    continue
                
                paths.append(path[:-4])  # remove .zst
                upload_info[stream] += size
                continue
    
    # convert to text
    upload_info = {k: f"{v / (1024*1024):.3f} MB" for k, v in upload_info.items()}
    print()
    print("uploaded files summary:")
    pprint(upload_info)
    
    paths.sort()  # make this ~deterministic
    return paths


def summarize_chunked_data(q2: list[tuple[str, int]]):
    # count the existing size of chunked data streams
    
    chunk_info = defaultdict(int)
    for chunk_path, size in q2:
        for stream in ALL_DATA_STREAMS_FILTER:
            if stream in chunk_path:
                chunk_info[stream] += size
                continue
    
    # convert to text
    chunk_info = {k: f"{v / (1024*1024):.3f} MB" for k, v in chunk_info.items()}
    print()
    print("chunked files summary:")
    pprint(chunk_info)
