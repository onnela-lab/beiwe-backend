from collections import defaultdict
from dateutil import tz

from database.data_access_models import ChunkRegistry
from database.tableau_api_models import DataQuantity
from database.user_models import Participant

ALLOWED_DATA_STREAMS = {'accelerometer', 'app_log'}  # TODO: add more. Maybe also make this on the DataQuantity model.


def calculate_data_quantity_stats(participant: Participant, study_timezone: tz.tzfile):
    """ Update the daily DataQuantity stats for a participant """
    chunkregistries = ChunkRegistry.objects.filter(participant=participant).all()
    daily_data_qtys = defaultdict(lambda: defaultdict(lambda: 0))
    for chunkregistry in chunkregistries:
        day = chunkregistry.time_bin.astimezone(study_timezone).date()
        daily_data_qtys[day][chunkregistry.data_type] += chunkregistry.file_size
    for day in daily_data_qtys:
        data_qty = {
            "participant": participant,
            "date": day,
        }
        for data_type in daily_data_qtys[day]:
            if data_type in ALLOWED_DATA_STREAMS:
                data_qty[data_type + '_bytes'] = daily_data_qtys[day][data_type]
        print(data_qty)
        DataQuantity.objects.update_or_create(**data_qty)
        # TODO: bulk update or create
        # TODO: use objects.exclude to delete those objects that shouldn't exist any longer
