from django.db import models
from django.utils import timezone

from database.common_models import TimestampedModel


class FileProcessingLockedError(Exception): pass


class FileProcessLock(TimestampedModel):
    """ This is used in old versions of data processing (and commendline data processing) to
     ensure overlapping processing runs do not occur. """

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

