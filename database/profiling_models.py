from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List

from django.db import models
from django.db.models import Count, Q, QuerySet, Sum
from django.utils import timezone

from constants.data_stream_constants import *
from database.common_models import UtilityModel
from database.models import JSONTextField, TimestampedModel
from database.user_models_participant import Participant


try:
    from database.study_models import Study  # Used in a type hint
except ImportError:
    pass

class EncryptionErrorMetadata(TimestampedModel):
    file_name = models.CharField(max_length=256)
    total_lines = models.PositiveIntegerField()
    number_errors = models.PositiveIntegerField()
    error_lines = JSONTextField()
    error_types = JSONTextField()
    participant: Participant = models.ForeignKey(Participant, on_delete=models.PROTECT, null=True)


class LineEncryptionError(TimestampedModel):
    AES_KEY_BAD_LENGTH = "AES_KEY_BAD_LENGTH"
    EMPTY_KEY = "EMPTY_KEY"
    INVALID_LENGTH = "INVALID_LENGTH"
    IV_BAD_LENGTH = "IV_BAD_LENGTH"
    IV_MISSING = "IV_MISSING"
    LINE_EMPTY = "LINE_EMPTY"
    LINE_IS_NONE = "LINE_IS_NONE"
    MALFORMED_CONFIG = "MALFORMED_CONFIG"
    MP4_PADDING = "MP4_PADDING"
    PADDING_ERROR = "PADDING_ERROR"
    
    ERROR_TYPE_CHOICES = (
        (AES_KEY_BAD_LENGTH, AES_KEY_BAD_LENGTH),
        (EMPTY_KEY, EMPTY_KEY),
        (INVALID_LENGTH, INVALID_LENGTH),
        (IV_BAD_LENGTH, IV_BAD_LENGTH),
        (IV_MISSING, IV_MISSING),
        (LINE_EMPTY, LINE_EMPTY),
        (LINE_IS_NONE, LINE_IS_NONE),
        (MP4_PADDING, MP4_PADDING),
        (MALFORMED_CONFIG, MALFORMED_CONFIG),
        (PADDING_ERROR, PADDING_ERROR),
    )
    
    type = models.CharField(max_length=32, choices=ERROR_TYPE_CHOICES)
    line = models.TextField(blank=True)
    base64_decryption_key = models.TextField()
    prev_line = models.TextField(blank=True)
    next_line = models.TextField(blank=True)
    participant: Participant = models.ForeignKey(Participant, null=True, on_delete=models.PROTECT)


# WARNING: this table is huge. Several-to-many multiples of ChunkRegistry, though it is not as
# complex and rows are individually less bulky. Never pull this table into memory, always use
# .iterator() in combination with .values() or .values_list() and test your query on your largest
# server to benchmark it.  This table is not indexed, it is for record keeping and debugging
# purposes, and it can be used to repopulate FilesToProcess with items to reprocess a participants
# data when something goes wrong.
# Participant.upload_trackers is probably the only safe way to access this, a participant probably
# will never upload a million files and shouldn't MemoryError your server to oblivion.


class UploadTracking(UtilityModel):
    file_path = models.CharField(max_length=256)
    file_size = models.PositiveIntegerField()
    timestamp = models.DateTimeField()
    participant: Participant = models.ForeignKey(Participant, on_delete=models.PROTECT, related_name='upload_trackers')
    
    def s3_retrieve(self):
        from libs.s3 import s3_retrieve
        return s3_retrieve(self.file_path, self.participant)
    
    @classmethod
    def re_add_files_to_process(cls, number=100):
        """ Re-adds the most recent [number] files that have been uploaded recently to FiletToProcess.
            (this is fairly optimized because it is part of debugging file processing) """
        uploads = cls.objects.order_by("-timestamp")[:number]
        cls._add_files_to_process(uploads)
    
    @classmethod
    def re_add_files_to_process_time(cls, time: datetime):
        """ re-adds files going back to a specific time. """
        uploads = cls.objects.filter(timestamp__gte=time).order_by("-timestamp")
        cls._add_files_to_process(uploads)
    
    @classmethod
    def _add_files_to_process(cls, uploads: QuerySet):
        from database.data_access_models import FileToProcess
        uploads = uploads.values_list(
            "file_path", "participant__study__object_id", "participant__study_id", "participant_id"
        )
        participant: Participant
        participant_cache: Dict[Participant] = {}  # cache participants
        file_paths = set(FileToProcess.objects.values_list("s3_file_path", flat=True)) # cache file paths
        
        new_ftps: List[FileToProcess] = []
        for i, (file_path, object_id, study_id, participant_id) in enumerate(uploads):
            if participant_id in participant_cache:
                participant = participant_cache[participant_id]
            else:
                participant = Participant.objects.get(id=participant_id)
                participant_cache[participant_id] = participant
            
            if i % 100 == 0:
                print(i, sep="... ")
            
            actual_file_path = object_id + "/" + file_path
            
            if actual_file_path in file_paths:
                print(f"skipping {actual_file_path}, appears to already be present")
                continue
            else:
                file_paths.add(actual_file_path)
            
            new_ftps.append(
                FileToProcess(
                    s3_file_path=actual_file_path,
                    study_id=study_id,
                    participant=participant,
                    os_type=participant.os_type,
                    app_version=participant.last_version_code or "1",  # TODO: implement historical version search
                )
            )
            
            if len(new_ftps) > 500:
                print("creating 500 new ftps")
                FileToProcess.objects.bulk_create(new_ftps)
                new_ftps = []
        
        print(f"creating {len(new_ftps)} new ftps")
        FileToProcess.objects.bulk_create(new_ftps)
    
    @classmethod
    def add_files_to_process2(cls, limit=25, data_stream=None):
        """ Re-adds the most recent [limit] files that have been uploaded recently to FiletToProcess.
            (this is fairly optimized because it is part of debugging file processing) """
        from database.data_access_models import FileToProcess
        data_streams = DATA_STREAM_TO_S3_FILE_NAME_STRING.values() if data_stream is None else [data_stream]
        upload_queries = []
        for ds in data_streams:
            if ds == "identifiers":
                continue
            query = (
                cls.objects.order_by("-timestamp")
                    .filter(file_path__contains=ds)
                    .values_list("file_path",
                                 "participant__study_id",
                                 "participant__study__object_id",
                                 "participant_id")[:limit]
            )
            upload_queries.append((ds, query))
        
        new_ftps = []
        # participant_cache = {}  # uhg need to cache participants...
        file_paths_wandered = set(FileToProcess.objects.values_list("s3_file_path", flat=True))
        for file_type, uploads_query in upload_queries:
            print(file_type)
            for i, (file_path, study_id, object_id, participant_id) in enumerate(uploads_query):
                
                if i % 10 == 0 or i == limit-1:
                    print(i+1 if i == limit-1 else i, sep="... ",)
                
                if file_path in file_paths_wandered:
                    continue
                else:
                    file_paths_wandered.add(file_path)
                
                new_ftps.append(FileToProcess(
                    s3_file_path=object_id + "/" + file_path,
                    study_id=study_id,
                    participant_id=participant_id,
                    # TODO: implement app version here
                ))
        FileToProcess.objects.bulk_create(new_ftps)
    
    @classmethod
    def reprocess_participant(cls, participant: Participant, destructive: bool, data_streams: list):
        """ Re-adds the most recent [limit] files that have been uploaded recently to FiletToProcess.
            (this is fairly optimized because it is part of debugging file processing) """
        from database.data_access_models import FileToProcess
        
        for dtype in data_streams:
            if dtype == IDENTIFIERS:
                raise ValueError("identifiers are not processed through data upload!!!")
            if dtype not in ALL_DATA_STREAMS:
                raise ValueError(f"invalid data type: {dtype}")
        
        if destructive:
            print("This action will remove all chunk registries that are not IDENTIFIERS for this participant.")
            y_n = input("Enter 'yes' to continue with destructive reprocessing.")
            if y_n != "yes":
                print("aborting.")
                return
            
            print("deleting chunk registry database entries for participant, this may take a while.")
            t1 = timezone.now()
            x = participant.chunk_registries.filter(data_type__in=data_streams).delete()
            t2 = timezone.now()
            print(f"deletion took {(t2-t1).total_seconds()} seconds.")
            print(f"output from deletion: {x}")
        
        # ordering by file path happens to be A) deterministic and B) sequential time order C)
        # results in ideal back-fill
        query = participant.upload_trackers.values_list("file_path", flat=True).order_by("file_path").distinct()
        participant_id = participant.id
        study_id = participant.study.id
        object_id = participant.study.object_id
        
        new_ftps: List[FileToProcess] = []
        extant_count = 0
        
        extant_file_paths = set(FileToProcess.objects.values_list("s3_file_path", flat=True))
        
        for i, upload_file_name in enumerate(query):
            s3_file_path = object_id + "/" + upload_file_name
            # track skipped count
            if s3_file_path in extant_file_paths:
                extant_count += 1
                if extant_count % 100 == 0:
                    print(f"skip count at {extant_count}", sep="... ")
                continue
            
            extant_file_paths.add(s3_file_path)
            
            # track progress, batch create every Nth ftp
            if i % 1000 == 0:
                print(i, sep="... ")
                FileToProcess.objects.bulk_create(new_ftps)
                new_ftps = []  # clear the list
            
            new_ftps.append(FileToProcess(
                s3_file_path=s3_file_path,
                study_id=study_id,
                participant_id=participant_id,
                # TODO: implement app version here
            ))
        
        # bulk save the overflow
        FileToProcess.objects.bulk_create(new_ftps)
    
    @classmethod
    def get_trailing_count(cls, time_delta) -> int:
        return cls.objects.filter(timestamp__gte=timezone.now() - time_delta).count()
    
    @classmethod
    def weekly_stats(cls, days=7, get_usernames=False):
        """ This gets a rough statement of data uploads and number of participants uploading data in
        the time range given. This is slow, only to be run in a shell manually. Do not attach this
        to an endpoint. """
        ALL_FILETYPES = UPLOAD_FILE_TYPE_MAPPING.values()
        if get_usernames:
            data = {filetype: {"megabytes": 0., "count": 0, "users": set()} for filetype in ALL_FILETYPES}
        else:
            data = {filetype: {"megabytes": 0., "count": 0} for filetype in ALL_FILETYPES}
        
        data["totals"] = {}
        data["totals"]["total_megabytes"] = 0
        data["totals"]["total_count"] = 0
        data["totals"]["users"] = set()
        days_delta = timezone.now() - timedelta(days=days)
        # .values is a huge speedup, .iterator isn't but it does let us print progress realistically
        query = UploadTracking.objects.filter(timestamp__gte=days_delta).values_list(
                "file_path", "file_size", "participant"
        ).iterator()
        
        for i, (file_path, file_size, participant) in enumerate(query):
            # global stats
            data["totals"]["total_count"] += 1
            data["totals"]["total_megabytes"] += file_size / 1024. / 1024.
            data["totals"]["users"].add(participant)
            
            # get data stream type from file_path (woops, ios log broke this code, fixed)
            path_extraction = file_path.split("/", 2)[1]
            if path_extraction == "ios":
                path_extraction = "ios_log"
            
            file_type = UPLOAD_FILE_TYPE_MAPPING[path_extraction]
            # update per-data-stream information
            data[file_type]["megabytes"] += file_size / 1024. / 1024.
            data[file_type]["count"] += 1
            
            if get_usernames:
                data[file_type]["users"].add(participant)
            if i % 10000 == 0:
                print("processed %s uploads..." % i)
        
        data["totals"]["user_count"] = len(data["totals"]["users"])
        
        if not get_usernames:  # purge usernames if we don't need them.
            del data["totals"]["users"]
        
        return data


class S3File(TimestampedModel):
    study: Study = models.ForeignKey("Study", on_delete=models.PROTECT, null=True, blank=True, related_name="s3_files")
    participant: Participant = models.ForeignKey("Participant", on_delete=models.PROTECT, null=True, blank=True, related_name="s3_files")
    path = models.TextField(unique=True)
    sha1 = models.BinaryField(max_length=20, null=True, blank=True)
    
    size_uncompressed = models.PositiveBigIntegerField(null=True, blank=True)
    size_compressed = models.PositiveBigIntegerField(null=True, blank=True)
    compression_time_ns = models.PositiveBigIntegerField(null=True, blank=True)
    decompression_time_ns = models.PositiveBigIntegerField(null=True, blank=True)
    encryption_time_ns = models.PositiveBigIntegerField(null=True, blank=True)
    download_time_ns = models.PositiveBigIntegerField(null=True, blank=True)
    upload_time_ns = models.PositiveBigIntegerField(null=True, blank=True)
    decrypt_time_ns = models.PositiveBigIntegerField(null=True, blank=True)
    
    object_ids = {}  # for caching study object id lookups
    
    # This is a mapping of data streams to strings found in their paths. it is based off real
    # values in the s3 bucket, which is a mess for historical reasons.
    MAPPING = {**UPLOAD_FILE_TYPE_MAPPING}
    MAPPING.update((stream, stream) for stream in ALL_DATA_STREAMS)
    MAPPING["/keys/"] = "key_file"
    MAPPING["forest"] = "forest"
    MAPPING["ios/log"] = IOS_LOG_FILE
    
    
    def get_object_id(self):
        # TODO: we can just logic this out of the path, we only allow study and CHUNKED_DATA
        from database.models import Study
        
        # first go through the object_ids cache, then study_id if present, then participant
        # study, populating the cache if we need to.
        self.study_id: int
        self.participant_id: int
        
        # if not self.study_id:
        #     if self.participant_id:
        #         Participant.value_get("study__object_id", pk=self.participant_id)
        #         self.study_id = self.participant.study_id
        #     else:
        #         return None
        object_ids = self.__class__.object_ids
        if self.study_id in object_ids:
            return object_ids[self.study_id]
        
        object_ids[self.study_id] = object_id = Study.value_get("object_id", pk=self.study_id)
        return object_id
    
    def __str__(self):
        return f"{self.path}"
    
    def get_data_stream(self):
        return self._get_data_stream(self.path)
    
    @classmethod
    def _get_data_stream(cls, path):
        """ Determines a data stream or file type based purely off the path. """
        
        determination = None
        for data_stream, the_name in cls.MAPPING.items():
            if data_stream in path:
                determination = the_name
                break
        
        # its something weird
        if not determination:
            return None
        
        # study object ids are 24 characters
        if len(path.split("/")[0]) == 24:
            determination = "raw_" + determination
        
        return determination
    
    def stats(self):
        if self.size_compressed and self.size_uncompressed:
            ratio = round(self.size_compressed / self.size_uncompressed, 2)
        else:
            ratio = None
        data_stream = self.get_data_stream() or self.path
        
        if self.study_id:  # type: ignore
            object_id = self.get_object_id()
            if self.path.startswith(object_id):
                print(f"data stream: raw {data_stream} - ratio: {ratio}")
        
        print(f"data stream: {data_stream} - ratio: {ratio}")
    
    @classmethod
    def global_ratio(cls):
        # using django annotations get the sum of all the compressed and uncompressed sizes,
        # print the ratio of compressed to uncompressed out to 4 decimal places.
        from django.db.models import Sum
        
        totals = cls.objects.aggregate(
            total_compressed=Sum("size_compressed"), total_uncompressed=Sum("size_uncompressed")
        )
        total_compressed = totals["total_compressed"]
        total_uncompressed = totals["total_uncompressed"]
        ratio = round(total_compressed / total_uncompressed, 4)
        print(ratio)
    
    @staticmethod
    def prant(label: str, compressed: int|None, uncompressed: int|None, count: int|None):
        if compressed is None and uncompressed is None:
            return
        
        if compressed is not None:
            compressed = round(compressed / 1024 / 1024, 4)  # type: ignore
        
        if uncompressed is not None:
            uncompressed = round(uncompressed / 1024 / 1024, 4)  # type: ignore
        
        if compressed is not None and uncompressed is not None:
            ratio = round(compressed / uncompressed *100, 4)
        else:
            ratio = "-"
        print(label, f"{ratio}% - ({count}) - {uncompressed}MB -> {compressed}MB")
    
    @classmethod
    def print_stats_slow(cls):
        """ Data from this can be used to build the mapping structure in _fast. """
        query = cls.objects.values_list("path", "size_uncompressed", "size_compressed")
        
        counter = defaultdict(int)
        data_streams = set()
        for i, (path, s_uncompressed, s_compressed) in enumerate(query.iterator()):
            if i % 100000 == 0:
                print(i)
            data_stream = cls._get_data_stream(path)
            data_streams.add(str(data_stream))
            if data_stream is None:
                print(path)
            counter[f"{data_stream} count"] += 1
            counter[f"{data_stream} uncompressed"] += s_uncompressed
            counter[f"{data_stream} compressed"] += s_compressed
        
        for k in sorted(data_streams):
            cls.prant(k, counter[f"{k} compressed"], counter[f"{k} uncompressed"], counter[f"{k} count"])
    
    @classmethod
    def print_stats_fast(cls):
        # label as the key, all strings to check for as the values.
        # it _should_ always be a map of ACCELEROMETER: ACCELEROMETER buuuuut we aren't that lucky.
        MAPPING = {
            ACCELEROMETER:    ("accel", ACCELEROMETER, ),
            AMBIENT_AUDIO:    ("ambientAudio", AMBIENT_AUDIO, ),
            ANDROID_LOG_FILE: ("logFile", ANDROID_LOG_FILE, ),
            BLUETOOTH:        ("bluetoothLog", BLUETOOTH, ),
            CALL_LOG:         ("callLog", CALL_LOG, ),
            DEVICEMOTION:     ("devicemotion", DEVICEMOTION, ),
            GPS:              ("gps", GPS, ),
            GYRO:             ("gyro", GYRO, ),
            IDENTIFIERS:      ("identifiers", IDENTIFIERS, ),
            IOS_LOG_FILE:     ("ios_log", "ios/log", IOS_LOG_FILE, ),
            MAGNETOMETER:     ("magnetometer", MAGNETOMETER, ),
            POWER_STATE:      ("powerState", POWER_STATE, ),
            PROXIMITY:        ("proximity", PROXIMITY, ),
            REACHABILITY:     ("reachability", REACHABILITY, ),
            SURVEY_ANSWERS:   ("surveyAnswers", SURVEY_ANSWERS, ),
            SURVEY_TIMINGS:   ("surveyTimings", SURVEY_TIMINGS, ),
            TEXTS_LOG:        ("textsLog", TEXTS_LOG, ),
            VOICE_RECORDING:  ("voiceRecording", VOICE_RECORDING, ),
            WIFI:             ("wifiLog", WIFI, ),
            "key_file":       ("/keys/", ),
            "forest":         ("forest", ),
        }
        
        for label, filters in MAPPING.items():
            path_contains = Q()
            for f in filters:
                path_contains |= Q(path__contains=f)
            
            # chunked data query
            compressed_chunked, uncompressed_chunked, count = S3File.objects.filter(
                path_contains, path__startswith="CHUNKED_DATA"
            ).aggregate(Sum("size_compressed"), Sum("size_uncompressed"), Count("pk")).values()
            
            cls.prant(label + "", compressed_chunked, uncompressed_chunked, count)
            
            # raw uplaod data query
            compressed_raw, uncompressed_raw, count_raw = S3File.objects.filter(path_contains)\
                .exclude(path__startswith="CHUNKED_DATA")\
                .aggregate(Sum("size_compressed"), Sum("size_uncompressed"), Count("pk")).values()
            
            cls.prant(label + " (raw)", compressed_raw, uncompressed_raw, count_raw)
            print()
