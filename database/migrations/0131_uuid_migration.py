# created manually, 3024-10-2

import uuid

from django.db import migrations, models
from django.db.migrations.state import StateApps


def add_uuids(apps: StateApps, schema_editor):
    """
    We need to add uuids to existing Scheduledevent objects.
    """
    ScheduledEvent = apps.get_model('database', 'ScheduledEvent')
    
    new_uuids = [
        ScheduledEvent(id=pk, uuid=uuid.uuid4()) for pk in
        ScheduledEvent.objects.filter(uuid__isnull=True, deleted=False).values_list('id', flat=True)
    ]
    ScheduledEvent.objects.bulk_update(new_uuids, ['uuid'], batch_size=3000)


def remove_uuids(apps: StateApps, schema_editor):
    """
    We need to remove uuids from existing Scheduledevent objects when migrating backwards (should
    just be for debugging? hope so!).
    """
    ScheduledEvent = apps.get_model('database', 'ScheduledEvent')
    ScheduledEvent.objects.all().update(uuid=None)


class Migration(migrations.Migration):
    
    dependencies = [
        ('database', '0130_appversionhistory_os_is_ios'),
    ]
    
    operations = [
        migrations.RunPython(add_uuids, reverse_code=remove_uuids),
        migrations.AlterField(
            model_name='scheduledevent',
            name='uuid',
            field=models.UUIDField(blank=True, db_index=True, default=uuid.uuid4, null=True, unique=True),
        ),
    ]
