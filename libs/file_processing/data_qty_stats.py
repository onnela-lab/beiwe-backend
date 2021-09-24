from collections import defaultdict

from config.constants import ALL_DATA_STREAMS
from database.data_access_models import ChunkRegistry
from database.tableau_api_models import DataQuantity
from database.user_models import Participant


def calculate_data_quantity_stats(participant: Participant):
    """ Update the daily DataQuantity stats for a participant, using ChunkRegistry data """
    chunkregistries = (ChunkRegistry.objects.filter(participant=participant)
                       .values('time_bin', 'data_type', 'file_size'))
    daily_data_qtys = defaultdict(lambda: defaultdict(lambda: 0))
    study_timezone = participant.study.timezone
    # Construct a dict formatted like this: dict[date][data_type] = total_bytes
    for chunkregistry in chunkregistries:
        day = chunkregistry['time_bin'].astimezone(study_timezone).date()
        daily_data_qtys[day][chunkregistry['data_type']] += chunkregistry['file_size']
    # Delete all existing DataQuantity objects for the participant
    DataQuantity.objects.filter(participant=participant).delete()
    # For each date, create a DataQuantity object
    data_qty_objects_to_be_created = []
    for day in daily_data_qtys:
        data_qty = {
            "participant": participant,
            "date": day,
        }
        for data_type in daily_data_qtys[day]:
            if data_type in ALL_DATA_STREAMS:
                data_qty[data_type + '_bytes'] = daily_data_qtys[day][data_type]
        data_qty_objects_to_be_created.append(DataQuantity(**data_qty))
    DataQuantity.objects.bulk_create(data_qty_objects_to_be_created)
    # TODO: if this needs to be optimized in the future, we could improve performance by only
    # querying ChunkRegistry objects that are newer than the last time we calculated all the
    # DataQuantity objects. But that would make the code significantly more complicated.
