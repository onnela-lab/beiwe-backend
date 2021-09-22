from datetime import datetime, timedelta
from dateutil import tz
from django.db import models

from database.data_access_models import ChunkRegistry


def calculate_data_quantity_stats(participant_id: int):
    earliest_chunk_registry = (ChunkRegistry.objects
                               .filter(participant_id=participant_id).earliest('time_bin'))
    print(earliest_chunk_registry.time_bin)  # Datetime


def get_data_qty(participant_id: int, day: datetime.date, study_timezone: datetime.tzinfo):
    """ Return the data quantity, in bytes, for a specific participant, data stream, and day. """
    start_time_local = datetime(day.year, day.month, day.day, tzinfo=study_timezone)
    end_time_local = start_time_local + timedelta(days=1)
    start_time = start_time_local.astimezone(tz.UTC)
    end_time = end_time_local.astimezone(tz.UTC)
    return (ChunkRegistry.objects
            .filter(participant_id=participant_id, time_bin__gte=start_time, time_bin__lt=end_time)
            .aggregate(models.Sum('file_size')))


# Participant.objects.annotate(totalbytes=Sum('chunk_registries__file_size')).values_list('patient_id', 'totalbytes').order_by('-totalbytes')

