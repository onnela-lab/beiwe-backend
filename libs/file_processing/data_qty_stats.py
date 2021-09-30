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
    """ Update the daily DataQuantity stats for a participant, using ChunkRegistry data
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

    chunkregistries = query.values('time_bin', 'data_type', 'file_size')
    daily_data_qtys = defaultdict(lambda: defaultdict(lambda: 0))
    # Construct a dict formatted like this: dict[date][data_type] = total_bytes
    for chunkregistry in chunkregistries:
        day = chunkregistry['time_bin'].astimezone(study_timezone).date()
        daily_data_qtys[day][chunkregistry['data_type']] += chunkregistry['file_size']
    # For each date, create a DataQuantity object
    data_qty_objects_to_be_created = []
    for day in daily_data_qtys:
        data_qty = {
            "participant": participant,
            "date": day,
            "defaults": {}
        }
        for data_type in daily_data_qtys[day]:
            if data_type in ALL_DATA_STREAMS:
                data_qty['defaults'][data_type + '_bytes'] = daily_data_qtys[day][data_type]
        SummaryStatisticDaily.objects.update_or_create(**data_qty)


def _utc_datetime_of_local_midnight_date(local_date, local_timezone):
    local_midnight = datetime.combine(local_date, datetime.min.time()).replace(tzinfo=local_timezone)
    return local_midnight.astimezone(utc)
