from collections import defaultdict
from datetime import datetime, timedelta
from pytz import utc
from typing import Optional

from config.constants import ALL_DATA_STREAMS, CHUNK_TIMESLICE_QUANTUM
from database.data_access_models import ChunkRegistry
from database.tableau_api_models import SummaryStatisticDaily
from database.user_models import Participant


def calculate_data_quantity_stats(
        participant: Participant,
        earliest_time_bin_number: Optional[int] = None,
        latest_time_bin_number: Optional[int] = None,
):
    """ Update the SummaryStatisticDaily  stats for a participant, using ChunkRegistry data
    earliest_time_bin_number -- expressed in hours since 1/1/1970
    latest_time_bin_number -- expressed in hours since 1/1/1970 """
    study_timezone = participant.study.timezone
    query = ChunkRegistry.objects.filter(participant=participant)

    # Filter by date range
    if earliest_time_bin_number is not None:
        start_datetime = datetime.utcfromtimestamp(earliest_time_bin_number * CHUNK_TIMESLICE_QUANTUM)
        # Round down to the beginning of the included day, in the study's timezone
        start_date = start_datetime.astimezone(study_timezone).date()
        query = query.filter(time_bin__gte=_utc_datetime_of_local_midnight_date(start_date, study_timezone))
    if latest_time_bin_number is not None:
        end_datetime = datetime.utcfromtimestamp(latest_time_bin_number * CHUNK_TIMESLICE_QUANTUM)
        # Round up to the beginning of the next day, in the study's timezone
        end_date = end_datetime.astimezone(study_timezone).date() + timedelta(days=1)
        query = query.filter(time_bin__lt=_utc_datetime_of_local_midnight_date(end_date, study_timezone))

    daily_data_quantities = defaultdict(lambda: defaultdict(int))
    # Construct a dict formatted like this: dict[date][data_type] = total_bytes
    for chunkregistry in query.values_list('time_bin', 'data_type', 'file_size'):
        day = chunkregistry[0].astimezone(study_timezone).date()
        daily_data_quantities[day][chunkregistry[1]] += chunkregistry[2]
    # For each date, create a DataQuantity object
    for day, day_data in daily_data_quantities.items():
        data_quantity = {
            "participant": participant,
            "date": day,
            "defaults": {}
        }
        for data_type, total_bytes in day_data.items():
            if data_type in ALL_DATA_STREAMS:
                data_quantity["defaults"][f"{data_type}_bytes"] = total_bytes
        SummaryStatisticDaily.objects.update_or_create(**data_quantity)


def _utc_datetime_of_local_midnight_date(local_date, local_timezone):
    local_midnight = datetime.combine(local_date, datetime.min.time()).replace(tzinfo=local_timezone)
    return local_midnight.astimezone(utc)
