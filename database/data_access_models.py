import json
import random
import string
from datetime import datetime

from django.db import models
from django.utils import timezone
from django_extensions.db.fields.json import JSONField

from config.constants import (CHUNK_TIMESLICE_QUANTUM, CHUNKABLE_FILES,
    PIPELINE_FOLDER)
from database.models import AbstractModel
from database.study_models import Study
from database.validators import LengthValidator
from libs.s3 import s3_retrieve
from libs.security import chunk_hash


class FileProcessingLockedError(Exception): pass
class UnchunkableDataTypeError(Exception): pass
class ChunkableDataTypeError(Exception): pass



class PipelineRegistry(AbstractModel):
    study = models.ForeignKey('Study', on_delete=models.PROTECT, related_name='pipeline_registries', db_index=True)
    participant = models.ForeignKey('Participant', on_delete=models.PROTECT, related_name='pipeline_registries', db_index=True)

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


class ChunkRegistry(AbstractModel):
    # this is declared in the abstract model but needs to be indexed for pipeline queries.
    last_updated = models.DateTimeField(auto_now=True, db_index=True)

    is_chunkable = models.BooleanField()
    chunk_path = models.CharField(max_length=256, db_index=True)  # , unique=True)
    chunk_hash = models.CharField(max_length=25, blank=True)

    # removed: data_type used to have choices of ALL_DATA_STREAMS, but this generated migrations
    # unnecessarily, so it has been removed.  This has no side effects.
    data_type = models.CharField(max_length=32, db_index=True)
    time_bin = models.DateTimeField(db_index=True)

    study = models.ForeignKey('Study', on_delete=models.PROTECT, related_name='chunk_registries', db_index=True)
    participant = models.ForeignKey('Participant', on_delete=models.PROTECT, related_name='chunk_registries', db_index=True)
    survey = models.ForeignKey('Survey', blank=True, null=True, on_delete=models.PROTECT, related_name='chunk_registries', db_index=True)

    file_size = models.IntegerField(null=True, default=None)

    def s3_retrieve(self):
        return s3_retrieve(self.chunk_path, self.study.object_id)

    @classmethod
    def register_chunked_data(cls, data_type, time_bin, chunk_path, file_contents, study_id,
                              participant_id, survey_id=None):
        
        if data_type not in CHUNKABLE_FILES:
            raise UnchunkableDataTypeError

        chunk_hash_str = chunk_hash(file_contents).decode()
        
        time_bin = int(time_bin) * CHUNK_TIMESLICE_QUANTUM
        time_bin = timezone.make_aware(datetime.utcfromtimestamp(time_bin), timezone.utc)
        # previous time_bin form was this:
        # datetime.fromtimestamp(time_bin)
        # On the server, but not necessarily in development environments, datetime.fromtimestamp(0)
        # provides the same date and time as datetime.utcfromtimestamp(0).
        # timezone.make_aware(datetime.utcfromtimestamp(0), timezone.utc) creates a time zone
        # aware datetime that is unambiguous in the UTC timezone and generally identecal timestamps.
        # Django's behavior (at least on this project, but this project is set to the New York
        # timezone so it should be generalizable) is to add UTC as a timezone when storing a naive
        # datetime in the database.
        
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
        # see comment in register_chunked_data above
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
    def get_chunks_time_range(cls, study_id, user_ids=None, data_types=None, start=None, end=None):
        """
        This function uses Django query syntax to provide datetimes and have Django do the
        comparison operation, and the 'in' operator to have Django only match the user list
        provided.
        """

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

    def update_chunk_hash(self, data_to_hash):
        self.chunk_hash = chunk_hash(data_to_hash)
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


class FileToProcess(AbstractModel):

    s3_file_path = models.CharField(max_length=256, blank=False)

    study = models.ForeignKey('Study', on_delete=models.PROTECT, related_name='files_to_process')
    participant = models.ForeignKey('Participant', on_delete=models.PROTECT, related_name='files_to_process')

    @classmethod
    def append_file_for_processing(cls, file_path, study_object_id, **kwargs):
        # Get the study's primary key
        study_pk = Study.objects.filter(object_id=study_object_id).values_list('pk', flat=True).get()
        
        if file_path[:24] == study_object_id:
            cls.objects.create(s3_file_path=file_path, study_id=study_pk, **kwargs)
        else:
            cls.objects.create(s3_file_path=study_object_id + '/' + file_path, study_id=study_pk, **kwargs)


class FileProcessLock(AbstractModel):
    
    lock_time = models.DateTimeField(null=True)
    
    @classmethod
    def lock(cls):
        if cls.islocked():
            raise FileProcessingLockedError('File processing already locked')
        else:
            cls.objects.create(lock_time=timezone.now())
    
    @classmethod
    def unlock(cls):
        cls.objects.all().delete()
    
    @classmethod
    def islocked(cls):
        return cls.objects.exists()
    
    @classmethod
    def get_time_since_locked(cls):
        return timezone.now() - FileProcessLock.objects.last().lock_time


class InvalidUploadParameterError(Exception): pass


class PipelineUpload(AbstractModel):
    REQUIREDS = [
        "study_id",
        "tags",
        "file_name",
    ]
    
    # no related name, this is
    object_id = models.CharField(max_length=24, unique=True, validators=[LengthValidator(24)])
    study = models.ForeignKey(Study, related_name="pipeline_uploads")
    file_name = models.TextField()
    s3_path = models.TextField()
    file_hash = models.CharField(max_length=128)

    @classmethod
    def get_creation_arguments(cls, params, file_object):
        errors = []

        # ensure required are present, we don't allow falsey contents.
        for field in PipelineUpload.REQUIREDS:
            if not params.get(field, None):
                errors.append('missing required parameter: "%s"' % field)

        # if we escape here early we can simplify the code that requires all parameters later
        if errors:
            raise InvalidUploadParameterError("\n".join(errors))

        # validate study_id
        study_id_object_id = params["study_id"]
        if not Study.objects.get(object_id=study_id_object_id):
            errors.append(
                'encountered invalid study_id: "%s"'
                % params["study_id"] if params["study_id"] else None
            )

        study_id = Study.objects.get(object_id=study_id_object_id).id

        if len(params['file_name']) > 256:
            errors.append("encountered invalid file_name, file_names cannot be more than 256 characters")

        if cls.objects.filter(file_name=params['file_name']).count():
            errors.append('a file with the name "%s" already exists' % params['file_name'])

        try:
            tags = json.loads(params["tags"])
            if not isinstance(tags, list):
                # must be json list, can't be json dict, number, or string.
                raise ValueError()
            if not tags:
                errors.append("you must provide at least one tag for your file.")
            tags = [str(_) for _ in tags]
        except ValueError:
            tags = None
            errors.append("could not parse tags, ensure that your uploaded list of tags is a json compatible array.")

        if errors:
            raise InvalidUploadParameterError("\n".join(errors))

        created_on = timezone.now()
        file_hash = chunk_hash(file_object.read())
        file_object.seek(0)

        s3_path = "%s/%s/%s/%s/%s" % (
            PIPELINE_FOLDER,
            params["study_id"],
            params["file_name"],
            created_on.isoformat(),
            ''.join(random.choice(string.ascii_letters + string.digits) for i in range(32)),
            # todo: file_name?
        )

        creation_arguments = {
            "created_on": created_on,
            "s3_path": s3_path,
            "study_id": study_id,
            "file_name": params["file_name"],
            "file_hash": file_hash,
        }

        return creation_arguments, tags


class PipelineUploadTags(AbstractModel):
    pipeline_upload = models.ForeignKey(PipelineUpload, related_name="tags")
    tag = models.TextField()
