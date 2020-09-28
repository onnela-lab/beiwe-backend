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



class FileAsText(TimestampedModel):
    tag = models.CharField(required=True, max_length=256, db_index=True)
    text = models.TextField(required=True)

    # todo: store the files for sending push notifications as three entries in this database table
    #  and make a function that checks for ios, android, and any push notifications to be present
    #  and returns a boolean.
    # todo: make a frontend page (system admins only) that allows you to upload (paste? whatever)
    #  makes more sense the three push notification credential files, and then VALIDATES them,
    #  tells the user what failed, and saves them.  (3 separate post operations, 1 each, not 3 in 1)


