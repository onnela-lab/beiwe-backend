from django.db import migrations

from database.user_models import Participant
from libs.file_processing.data_qty_stats import calculate_data_quantity_stats


def populate_historical_data_qty_stats(apps, schema_editor):
    for participant in Participant.objects.all():
        calculate_data_quantity_stats(participant)


class Migration(migrations.Migration):
    dependencies = [
        ('database', '0060_data_qty_stats'),
    ]

    operations = [
        migrations.RunPython(populate_historical_data_qty_stats),
    ]
