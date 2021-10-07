from collections import defaultdict
from dateutil.tz import gettz
from django.db import migrations

from config.constants import ALL_DATA_STREAMS


def populate_historical_data_qty_stats(apps, schema_editor):
    ChunkRegistry = apps.get_model('database', 'ChunkRegistry')
    Participant = apps.get_model('database', 'Participant')
    SummaryStatisticDaily = apps.get_model('database', 'SummaryStatisticDaily')

    def calculate_data_quantity_stats(participant: Participant):
        """ Update the SummaryStatisticDaily  stats for a participant, using ChunkRegistry data """
        study_timezone = gettz(participant.study.timezone_name)
        query = ChunkRegistry.objects.filter(participant=participant)

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

    for participant in Participant.objects.all():
        calculate_data_quantity_stats(participant)


class Migration(migrations.Migration):
    dependencies = [
        ('database', '0060_data_qty_stats'),
    ]

    operations = [
        migrations.RunPython(populate_historical_data_qty_stats),
    ]
