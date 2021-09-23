from collections import defaultdict
from django.db.models import Q

from config.constants import ALL_DATA_STREAMS
from database.data_access_models import ChunkRegistry
from database.tableau_api_models import DataQuantity
from database.user_models import Participant


def calculate_data_quantity_stats(participant: Participant):
    """ Update the daily DataQuantity stats for a participant, using ChunkRegistry data """
    chunkregistries = ChunkRegistry.objects.filter(participant=participant).all()
    daily_data_qtys = defaultdict(lambda: defaultdict(lambda: 0))
    study_timezone = participant.study.timezone
    # Construct a dict formatted like this: dict[date][data_type] = total_bytes
    for chunkregistry in chunkregistries:
        day = chunkregistry.time_bin.astimezone(study_timezone).date()
        daily_data_qtys[day][chunkregistry.data_type] += chunkregistry.file_size
    # For each date, create a DataQuantity object
    for day in daily_data_qtys:
        data_qty = {
            "participant": participant,
            "date": day,
        }
        for data_type in daily_data_qtys[day]:
            if data_type in ALL_DATA_STREAMS:
                data_qty[data_type + '_bytes'] = daily_data_qtys[day][data_type]
        DataQuantity.objects.update_or_create(**data_qty)
    # Delete any DataQuantity objects that shouldn't exist, i.e. from days with no ChunkRegistries
    all_days_with_data_query = Q()
    for day in daily_data_qtys:
        all_days_with_data_query |= Q(date=day)
    DataQuantity.objects.filter(participant=participant).exclude(all_days_with_data_query).delete()
    # TODO: bulk update or create
