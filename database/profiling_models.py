from datetime import timedelta

from django.db import models
from django.utils import timezone

from config.constants import UPLOAD_FILE_TYPE_MAPPING
from database.models import JSONTextField, Participant, TimestampedModel
from libs.security import decode_base64


class EncryptionErrorMetadata(TimestampedModel):
    file_name = models.CharField(max_length=256)
    total_lines = models.PositiveIntegerField()
    number_errors = models.PositiveIntegerField()
    error_lines = JSONTextField()
    error_types = JSONTextField()
    participant = models.ForeignKey('Participant', on_delete=models.PROTECT, null=True)


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
    participant = models.ForeignKey(Participant, null=True, on_delete=models.PROTECT)


class DecryptionKeyError(TimestampedModel):
    file_path = models.CharField(max_length=256)
    contents = models.TextField()
    traceback = models.TextField(null=True)
    participant = models.ForeignKey('Participant', on_delete=models.PROTECT, related_name='decryption_key_errors')
    
    def decode(self):
        return decode_base64(self.contents)


class UploadTracking(TimestampedModel):
    file_path = models.CharField(max_length=256)
    file_size = models.PositiveIntegerField()
    timestamp = models.DateTimeField()
    participant = models.ForeignKey('Participant', on_delete=models.PROTECT, related_name='upload_trackers')

    @classmethod
    def re_add_files_to_process(cls, number=100):
        """ Re-adds the most recent [number] files that have been uploaded recently to FiletToProcess.
            (this is fairly optimized because it is part of debugging file processing) """
        from database.data_access_models import FileToProcess
        uploads = cls.objects.order_by("-created_on").values_list(
            "file_path", "participant__study__object_id", "participant_id"
        )[:number]

        participant_cache = {}  # uhg need to cache participants...
        for i, (file_path, participant__study__object_id, participant_id) in enumerate(uploads):
            if participant_id in participant_cache:
                participant = participant_cache[participant_id]
            else:
                participant = Participant.objects.get(id=participant_id)
                participant_cache[participant_id] = participant

            if i % 10 == 0:
                print(i, sep="... ")

            if FileToProcess.objects.filter(s3_file_path__icontains=file_path).exists():
                print(f"skipping {file_path}, appears to already be present")
                continue

            FileToProcess.append_file_for_processing(
                # file_path, study_object_id, **kwargs
                file_path,
                participant__study__object_id,
                participant=participant,
            )

    @classmethod
    def get_trailing_count(cls, time_delta):
        return cls.objects.filter(timestamp__gte=timezone.now() - time_delta).count()


    @classmethod
    def weekly_stats(cls, days=7, get_usernames=False):
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
        query = UploadTracking.objects.filter(timestamp__gte=days_delta).values(
                "file_path", "file_size", "participant"
        ).iterator()
        
        for i, upload in enumerate(query):
            # global stats
            data["totals"]["total_count"] += 1
            data["totals"]["total_megabytes"] += upload["file_size"]/ 1024. / 1024.
            data["totals"]["users"].add(upload["participant"])
            
            # get data stream type from file_path (woops, ios log broke this code, fixed)
            path_extraction = upload["file_path"].split("/", 2)[1]
            if path_extraction == "ios":
                path_extraction = "ios_log"
                
            file_type = UPLOAD_FILE_TYPE_MAPPING[path_extraction]
            # update per-data-stream information
            data[file_type]["megabytes"] += upload["file_size"]/ 1024. / 1024.
            data[file_type]["count"] += 1
            
            if get_usernames:
                data[file_type]["users"].add(upload["participant"])
            if i % 10000 == 0:
                print("processed %s uploads..." % i)
        
        data["totals"]["user_count"] = len(data["totals"]["users"])
        
        if not get_usernames:  # purge usernames if we don't need them.
            del data["totals"]["users"]
        
        return data
