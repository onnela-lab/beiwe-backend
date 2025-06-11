from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, tzinfo

from dateutil.tz import UTC
from django.db.models.query import QuerySet
from django.utils.timezone import make_aware

from constants.data_processing_constants import CHUNK_TIMESLICE_QUANTUM
from constants.data_stream_constants import ALL_DATA_STREAMS
from database.data_access_models import ChunkRegistry
from database.forest_models import SummaryStatisticDaily
from database.user_models_participant import Participant
from libs.utils.date_utils import date_to_end_of_day, date_to_start_of_day, get_timezone_shortcode


# not importable
utcfromtimestamp: Callable = datetime.utcfromtimestamp


def timeslice_to_start_of_day(timeslice: int, tz: tzinfo):
    """ We use an integer to represent time, it must be multiplied by CHUNK_TIMESLICE_QUANTUM to
    yield a unix timestamp."""
    # get the date _in the local time_, then get the start of that day as a datetime
    day = make_aware(utcfromtimestamp(timeslice * CHUNK_TIMESLICE_QUANTUM), UTC) \
        .astimezone(tz).date()
    return date_to_start_of_day(day, tz)


def timeslice_to_end_of_day(timeslice: int, tz: tzinfo):
    """ We use an integer to represent time, it must be multiplied by CHUNK_TIMESLICE_QUANTUM to
    yield a unix timestamp."""
    # get the date _in the local time_, then get the end of that day as a datetime
    day = make_aware(utcfromtimestamp(timeslice * CHUNK_TIMESLICE_QUANTUM), UTC) \
        .astimezone(tz).date()
    return date_to_end_of_day(day, tz)


def populate_data_quantity(
    chunkregistry_query: QuerySet, study_timezone: tzinfo
) -> dict[datetime, dict[str, int]]:
    # Constructs a dict formatted like this: dict[date][data_type] = total_bytes
    daily_data_quantities = defaultdict(lambda: defaultdict(int))
    time_bin: datetime
    chunk_data_type: str
    file_size: int
    fields = ('time_bin', 'data_type', 'file_size')
    # get the date in the study's timezone, identify the size of that chunk, add based on data type
    for time_bin, chunk_data_type, file_size in chunkregistry_query.values_list(*fields):
        day = time_bin.astimezone(study_timezone).date()
        file_size = 0 if file_size is None else file_size
        daily_data_quantities[day][chunk_data_type] += file_size
    
    return daily_data_quantities


def calculate_data_quantity_stats(
    participant: Participant,
    earliest_time_bin_number: int|None = None,
    latest_time_bin_number: int|None = None,
):
    """ Update the SummaryStatisticDaily  stats for a participant, using ChunkRegistry data
    earliest_time_bin_number -- expressed in hours since 1/1/1970
    latest_time_bin_number -- expressed in hours since 1/1/1970 """
    
    # they can be equal, but they have to be populated or else the query is unbounded
    if earliest_time_bin_number is None or latest_time_bin_number is None:
        return
    
    # (related model, study, is cached)
    study_timezone: tzinfo = participant.study.timezone
    query = ChunkRegistry.objects.filter(participant=participant)
    
    # Filter by date range
    query = query.filter(
        time_bin__gte=timeslice_to_start_of_day(earliest_time_bin_number, study_timezone),
        time_bin__lt=timeslice_to_end_of_day(latest_time_bin_number, study_timezone)
    )
    
    # For each date, create a DataQuantity object
    for day, day_data in populate_data_quantity(query, study_timezone).items():
        data_quantity = {
            "participant_id": participant.pk,
            "date": day,
            "defaults": {
                "timezone": get_timezone_shortcode(day, study_timezone)
            }
        }
        for data_type, total_bytes in day_data.items():
            if data_type in ALL_DATA_STREAMS:
                data_quantity["defaults"][f"beiwe_{data_type}_bytes"] = total_bytes
        SummaryStatisticDaily.objects.update_or_create(**data_quantity)
