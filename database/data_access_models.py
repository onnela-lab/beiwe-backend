from __future__ import annotations

from collections import Counter
from datetime import datetime, UTC
from typing import TYPE_CHECKING

from django.db.models import (BooleanField, CharField, DateTimeField, ForeignKey, IntegerField,
    PROTECT, QuerySet)

from constants.common_constants import EARLIEST_POSSIBLE_DATA_DATETIME
from constants.data_processing_constants import CHUNK_TIMESLICE_QUANTUM
from constants.data_stream_constants import CHUNKABLE_FILES
from constants.user_constants import OS_TYPE_CHOICES
from database.models import Participant, TimestampedModel
from libs.utils.security_utils import chunk_hash


if TYPE_CHECKING:
    from database.models import Study, Survey


class UnchunkableDataTypeError(Exception): pass
class ChunkableDataTypeError(Exception): pass


#
# BIG FAT WARNING: the ChunkRegistry gets Huge. If you are in the context of a webserver endpoint
# and querying it for any other purpose than downloading files from s3 then you are doing it wrong,
# and you may even lock up the website due to database contention.  You should instead query summary
# SummaryStatistics. Any queries to ChunkRegistry should use .iterator() and .values_list() in
# carefully constructed queries to avoid loading extra objects into memory, both in Python and on
# the database. The Dashboard pages make use of this pattern, but are at time of commenting a bit
# messy and can be further optimized.
#


class ChunkRegistry(TimestampedModel):
    # the last_updated field's index legacy, removing it is slow to deploy on large servers.
    # TODO: remove this db_index? it doesn't harm anything...
    last_updated = DateTimeField(auto_now=True, db_index=True)
    is_chunkable = BooleanField()
    chunk_path = CharField(max_length=256, db_index=True, unique=True)
    chunk_hash = CharField(max_length=25, blank=True)
    
    # removed: data_type used to have choices of ALL_DATA_STREAMS, but this generated migrations
    # unnecessarily, so it has been removed.  This has no side effects.
    # TODO: the above comment is incorrect, we have on-database-save validation, revert to include choices
    data_type = CharField(max_length=32, db_index=True)
    time_bin = DateTimeField(db_index=True)
    file_size = IntegerField(null=True, default=None)  # Size (in bytes) of the (uncompressed) file, off by 16 bytes because of encryption iv
    study: Study = ForeignKey(  # type: ignore
        'Study', on_delete=PROTECT, related_name='chunk_registries', db_index=True
    )
    participant: Participant = ForeignKey(  # type: ignore
        'Participant', on_delete=PROTECT, related_name='chunk_registries', db_index=True
    )
    survey: Survey = ForeignKey(  # type: ignore
        'Survey', blank=True, null=True, on_delete=PROTECT, related_name='chunk_registries',
        db_index=True
    )
    
    def s3_retrieve(self) -> bytes:
        from libs.s3 import s3_retrieve
        return s3_retrieve(self.chunk_path, self.study.object_id, raw_path=True)
    
    register_required = {
        "study_id": int,
        "participant_id": int,
        "data_type": str,
        "chunk_path": str,
        "chunk_hash": str | bytes,
        "time_bin": int,
        "survey_id": int | None,
        "file_size": int,
    }
    
    @classmethod
    def register_chunked_data(cls, **kwargs):
        # validate required kwargs, extra kwargs not present, data type is chunkable
        if missing := [k for k in cls.register_required if k not in kwargs]:
            raise ValueError(f"Missing required parameters: {missing}")
        if unexpected:= [k for k in kwargs if k not in cls.register_required]:
            raise ValueError(f"Unexpected parameters: {unexpected}")
        if kwargs["data_type"] not in CHUNKABLE_FILES:
            raise UnchunkableDataTypeError(f"Data type '{kwargs['data_type']}' is not chunkable.")
        
        time_bin = kwargs["time_bin"]  # determine time bin...
        time_bin = int(time_bin) * CHUNK_TIMESLICE_QUANTUM  # best variable name ever...
        time_bin = datetime.fromtimestamp(time_bin, UTC)
        kwargs["time_bin"] = time_bin
        
        if isinstance(kwargs["chunk_hash"], bytes):  # just fix type on chunk hash because
            kwargs["chunk_hash"] = kwargs["chunk_hash"].decode()
        
        ChunkRegistry(is_chunkable=True, **kwargs).save()  # create with validation
    
    @classmethod
    def register_unchunked_data(
        cls,
        data_type: str,
        unix_timestamp: int,
        chunk_path: str,
        study_id: int,
        participant_id: int,
        file_contents: bytes,
        survey_id=None
    ):
        time_bin = datetime.fromtimestamp(unix_timestamp, UTC)
        
        if data_type in CHUNKABLE_FILES:
            raise ChunkableDataTypeError
        
        cls.objects.create(
            is_chunkable=False,
            chunk_path=chunk_path,
            chunk_hash=chunk_hash(file_contents),
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
    def get_chunks_time_range(
        cls, study_id, participant_ids=None, data_types=None, start=None, end=None
    ) -> QuerySet[ChunkRegistry]:
        """This function uses Django query syntax to provide datetimes and have Django do the
        comparison operation, and the 'in' operator to have Django only match the user list
        provided. """
        query = {'study_id': study_id}
        if participant_ids:
            query['participant__patient_id__in'] = participant_ids
        if data_types:
            query['data_type__in'] = data_types
        if start:
            query['time_bin__gte'] = start
        if end:
            query['time_bin__lte'] = end
        return cls.objects.filter(**query)
    
    @classmethod
    def get_updated_users_for_study(cls, study, date_of_last_activity) -> QuerySet[str]:
        """ Returns a list of patient ids that have had new or updated ChunkRegistry data
        since the datetime provided. """
        # note that date of last activity is actually date of last data processing operation on the
        # data uploaded by a user.
        return cls.objects.filter(
            study=study, last_updated__gte=date_of_last_activity
        ).values_list("participant__patient_id", flat=True).distinct()
    
    @classmethod
    def exclude_bad_time_bins(cls) -> QuerySet[ChunkRegistry]:
        # roughly one month before beiwe launch date
        return cls.objects.exclude(time_bin__lt=EARLIEST_POSSIBLE_DATA_DATETIME)


class FileToProcess(TimestampedModel):
    # this should have a max length of 66 characters on audio recordings
    s3_file_path = CharField(max_length=256, blank=False, unique=True)
    study: Study = ForeignKey('Study', on_delete=PROTECT, related_name='files_to_process')
    participant: Participant = ForeignKey('Participant', on_delete=PROTECT, related_name='files_to_process')
    os_type = CharField(max_length=16, choices=OS_TYPE_CHOICES, blank=True, null=False, default="")
    app_version = CharField(max_length=16, blank=True, null=False, default="")
    deleted = BooleanField(default=False)
    
    def s3_retrieve(self) -> bytes:
        return s3_retrieve(self.s3_file_path, self.study, raw_path=True)
    
    @staticmethod
    def ensure_study_prefix_in_path(file_path: str, study_object_id: str) -> str:
        """ whatever the reason for this file path transform is has been lost to the mists of time.
            We force the start of the path to the object id string of the study. """
        if file_path[:24] == study_object_id:
            return file_path
        else:
            return study_object_id + '/' + file_path
    
    @classmethod
    def test_file_path_exists(cls, file_path: str, study_object_id: str) -> bool:
        # identifies whether the provided file path currently exists.
        # we get terrible performance issues in data processing when duplicate files are present
        # in FileToProcess. We added a unique constraint and need to test the condition.
        return cls.objects.filter(
            s3_file_path=cls.ensure_study_prefix_in_path(file_path, study_object_id)
        ).exists()
    
    @classmethod
    def append_file_for_processing(cls, file_path: str, participant: Participant):
        # normalize the file path, grab the study id, passthrough kwargs to create; create.
        cls.objects.create(
            s3_file_path=cls.ensure_study_prefix_in_path(file_path, participant.study.object_id),
            participant=participant,
            study=participant.study,
            os_type=participant.os_type,
            app_version=participant.last_version_code or "",
        )
    
    @classmethod
    def report(cls, *args, **kwargs) -> dict[str, int]:
        return dict(
            reversed(
                Counter(FileToProcess.objects.values_list("participant__patient_id", flat=True)).most_common()
            )
        )
