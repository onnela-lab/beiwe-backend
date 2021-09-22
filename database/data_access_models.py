from datetime import datetime, timedelta

from django.db import models
from django.utils import timezone
from django_extensions.db.fields.json import JSONField

from config.constants import (API_TIME_FORMAT, CHUNK_TIMESLICE_QUANTUM, CHUNKABLE_FILES,
    CHUNKS_FOLDER, IDENTIFIERS, REVERSE_UPLOAD_FILE_TYPE_MAPPING)
from database.models import TimestampedModel
from database.study_models import Study
from database.user_models import Participant
from database.validators import LengthValidator
from libs.s3 import s3_list_files, s3_retrieve
from libs.security import chunk_hash


class UnchunkableDataTypeError(Exception): pass
class ChunkableDataTypeError(Exception): pass


class PipelineRegistry(TimestampedModel):
    study = models.ForeignKey(
        'Study', on_delete=models.PROTECT, related_name='pipeline_registries', db_index=True
    )
    participant = models.ForeignKey(
        'Participant', on_delete=models.PROTECT, related_name='pipeline_registries', db_index=True
    )
    data_type = models.CharField(max_length=256, db_index=True)
    processed_data = JSONField(null=True, blank=True)
    uploaded_at = models.DateTimeField(db_index=True)

    @classmethod
    def register_pipeline_data(cls, study, participant_id, data, data_type):
        cls.objects.create(
            study=study,
            participant_id=participant_id,
            processed_data=data,
            data_type=data_type,
            uploaded_at=datetime.utcnow(),
        )


class ChunkRegistry(TimestampedModel):
    # this is declared in the abstract model but needs to be indexed for pipeline queries.
    last_updated = models.DateTimeField(auto_now=True, db_index=True)
    is_chunkable = models.BooleanField()
    chunk_path = models.CharField(max_length=256, db_index=True, unique=True)
    chunk_hash = models.CharField(max_length=25, blank=True)

    # removed: data_type used to have choices of ALL_DATA_STREAMS, but this generated migrations
    # unnecessarily, so it has been removed.  This has no side effects.
    data_type = models.CharField(max_length=32, db_index=True)
    time_bin = models.DateTimeField(db_index=True)
    file_size = models.IntegerField(null=True, default=None)  # Size (in bytes) of the uncompressed file
    study = models.ForeignKey(
        'Study', on_delete=models.PROTECT, related_name='chunk_registries', db_index=True
    )
    participant = models.ForeignKey(
        'Participant', on_delete=models.PROTECT, related_name='chunk_registries', db_index=True
    )
    survey = models.ForeignKey(
        'Survey', blank=True, null=True, on_delete=models.PROTECT, related_name='chunk_registries',
        db_index=True
    )

    def s3_retrieve(self):
        return s3_retrieve(self.chunk_path, self.study.object_id, raw_path=True)

    @classmethod
    def register_chunked_data(
            cls, data_type, time_bin, chunk_path, file_contents, study_id, participant_id, survey_id=None
    ):
        if data_type not in CHUNKABLE_FILES:
            raise UnchunkableDataTypeError

        chunk_hash_str = chunk_hash(file_contents).decode()
        time_bin = int(time_bin) * CHUNK_TIMESLICE_QUANTUM
        time_bin = timezone.make_aware(datetime.utcfromtimestamp(time_bin), timezone.utc)

        cls.objects.create(
            is_chunkable=True,
            chunk_path=chunk_path,
            chunk_hash=chunk_hash_str,
            data_type=data_type,
            time_bin=time_bin,
            study_id=study_id,
            participant_id=participant_id,
            survey_id=survey_id,
            file_size=len(file_contents),
        )

    @classmethod
    def register_unchunked_data(cls, data_type, unix_timestamp, chunk_path, study_id, participant_id,
                                file_contents, survey_id=None):
        time_bin = timezone.make_aware(datetime.utcfromtimestamp(unix_timestamp), timezone.utc)

        if data_type in CHUNKABLE_FILES:
            raise ChunkableDataTypeError

        cls.objects.create(
            is_chunkable=False,
            chunk_path=chunk_path,
            chunk_hash='',
            data_type=data_type,
            time_bin=time_bin,
            study_id=study_id,
            participant_id=participant_id,
            survey_id=survey_id,
            file_size=len(file_contents),
        )

    @classmethod
    def update_registered_unchunked_data(cls, data_type, chunk_path, file_contents):
        """ Updates the data in case a user uploads an unchunkable file more than once,
        and updates the file size just in case it changed. """
        if data_type in CHUNKABLE_FILES:
            raise ChunkableDataTypeError
        chunk = cls.objects.get(chunk_path=chunk_path)
        chunk.file_size = len(file_contents)
        chunk.save()

    @classmethod
    def get_chunks_time_range(cls, study_id, user_ids=None, data_types=None, start=None, end=None):
        """This function uses Django query syntax to provide datetimes and have Django do the
        comparison operation, and the 'in' operator to have Django only match the user list
        provided. """
        query = {'study_id': study_id}
        if user_ids:
            query['participant__patient_id__in'] = user_ids
        if data_types:
            query['data_type__in'] = data_types
        if start:
            query['time_bin__gte'] = start
        if end:
            query['time_bin__lte'] = end
        return cls.objects.filter(**query)

    def update_chunk(self, data_to_hash: bytes):
        self.chunk_hash = chunk_hash(data_to_hash).decode()
        self.save()

    @classmethod
    def get_updated_users_for_study(cls, study, date_of_last_activity):
        """ Returns a list of patient ids that have had new or updated ChunkRegistry data
        since the datetime provided. """
        # note that date of last activity is actually date of last data processing operation on the
        # data uploaded by a user.
        return cls.objects.filter(
            study=study, last_updated__gte=date_of_last_activity
        ).values_list("participant__patient_id", flat=True).distinct()


class FileToProcess(TimestampedModel):
    s3_file_path = models.CharField(max_length=256, blank=False, unique=True)
    study = models.ForeignKey('Study', on_delete=models.PROTECT, related_name='files_to_process')
    participant = models.ForeignKey('Participant', on_delete=models.PROTECT, related_name='files_to_process')
    deleted = models.BooleanField(default=False)

    @staticmethod
    def normalize_s3_file_path(file_path: str, study_object_id: str):
        """ whatever the reason for this file path transform is has been lost to the mists of time.
            We force the start of the path to the object id string of the study. """
        if file_path[:24] == study_object_id:
            return file_path
        else:
            return study_object_id + '/' + file_path

    @classmethod
    def test_file_path_exists(cls, file_path: str, study_object_id: str):
        # identifies whether the provided file path currently exists.
        # we get terrible performance issues in data processing when duplicate files are present
        # in FileToProcess. We added a unique constraint and need to test the condition.
        return cls.objects.filter(
            s3_file_path=cls.normalize_s3_file_path(file_path, study_object_id)
        ).exists()

    @classmethod
    def append_file_for_processing(cls, file_path: str, study_object_id: str, **kwargs):
        # normalize the file path, grab the study id, passthrough kwargs to create; create.
        cls.objects.create(
            s3_file_path=cls.normalize_s3_file_path(file_path, study_object_id),
            study_id=Study.objects.filter(object_id=study_object_id).values_list('pk', flat=True).get(),
            **kwargs
        )

    @classmethod
    def reprocess_originals_from_chunk_path(cls, chunk_path):
        """ Takes a processed file (chunk) s3 path, identifies the original source files,
        and prepares a FileToProcess entry so that the source data will be re-processed
        and merged into the existing data.
        This is mostly a utility function, it was originally part of a script, but it is
        quite complex to accomplish, and worth holding on to.
        Contains print statements. """
        path_components = chunk_path.split("/")
        if len(path_components) != 5:
            raise Exception("chunked file paths contain exactly 5 components separated by a slash.")

        chunk_files_text, study_obj_id, username, data_stream, timestamp = path_components

        if not chunk_files_text == CHUNKS_FOLDER:
            raise Exception("This is not a chunked file, it is not in the chunked data folder.")

        participant = Participant.objects.get(patient_id=username)

        # data stream names are truncated
        full_data_stream = REVERSE_UPLOAD_FILE_TYPE_MAPPING[data_stream]

        # oh good, identifiers doesn't end in a slash.
        splitter_end_char = '_' if full_data_stream == IDENTIFIERS else '/'
        file_prefix = "/".join((study_obj_id, username, full_data_stream,)) + splitter_end_char

        # find all files with data from the appropriate time.
        dt_start = datetime.strptime(timestamp.strip(".csv"), API_TIME_FORMAT)
        dt_prev = dt_start - timedelta(hours=1)
        dt_end = dt_start + timedelta(hours=1)
        prior_hour_last_file = None
        file_paths_to_reprocess = []
        for s3_file_path in s3_list_files(file_prefix, as_generator=False):
            # convert timestamp....
            if full_data_stream == IDENTIFIERS:
                file_timestamp = float(s3_file_path.rsplit(splitter_end_char)[-1][:-4])
            else:
                file_timestamp = float(s3_file_path.rsplit(splitter_end_char)[-1][:-4]) / 1000
            file_dt = datetime.fromtimestamp(file_timestamp)
            # we need to get the last file from the prior hour as it my have relevant data,
            # fortunately returns of file paths are in ascending order, so it is the file
            # right before the rest of the data.  just cache it
            if dt_prev <= file_dt < dt_start:
                prior_hour_last_file = s3_file_path

            # and then every file within the relevant hour
            if dt_start <= file_dt <= dt_end:
                print("found:", s3_file_path)
                file_paths_to_reprocess.append(s3_file_path)

        # a "should be an unnecessary" safety check, but apparently we can't have nice things.
        if prior_hour_last_file and prior_hour_last_file not in file_paths_to_reprocess:
            print("found:", prior_hour_last_file)
            file_paths_to_reprocess.append(prior_hour_last_file)

        if not prior_hour_last_file and not file_paths_to_reprocess:
            raise Exception(  # this should not happen...
                f"did not find any matching files: '{chunk_path}' using prefix '{file_prefix}'"
            )

        for fp in file_paths_to_reprocess:
            if cls.objects.filter(s3_file_path=fp).exists():
                print(f"{fp} is already queued for processing")
                continue
            else:
                print(f"Adding {fp} as a file to reprocess.")
                cls.append_file_for_processing(fp, study_obj_id, participant=participant)


# Everything below this line should [only] be deleting by reverting the correct commit.
class InvalidUploadParameterError(Exception): pass


class PipelineUpload(TimestampedModel):
    # no related name, this is
    object_id = models.CharField(max_length=24, unique=True, validators=[LengthValidator(24)])
    study = models.ForeignKey(Study, related_name="pipeline_uploads", on_delete=models.PROTECT)
    file_name = models.TextField()
    s3_path = models.TextField()
    file_hash = models.CharField(max_length=128)


class PipelineUploadTags(TimestampedModel):
    pipeline_upload = models.ForeignKey(PipelineUpload, related_name="tags", on_delete=models.CASCADE)
    tag = models.TextField()

