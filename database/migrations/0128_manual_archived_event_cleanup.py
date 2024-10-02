from django.db import migrations
from django.db.migrations.state import StateApps

from constants.message_strings import (ACCOUNT_NOT_FOUND, CONNECTION_ABORTED,
    FAILED_TO_ESTABLISH_CONNECTION, UNEXPECTED_SERVICE_RESPONSE, UNKNOWN_REMOTE_ERROR)


def clean_archived_events(apps: StateApps, schema_editor):
    # discovered some problem data in our database, hopefully this clears it up
    ArchivedEvent = apps.get_model('database', 'ArchivedEvent')
    q_base = ArchivedEvent.objects
    
    # based on the special casing in failed_send_survey_handler in celery push notifications
    doctype = q_base.filter(status__icontains="DOCTYPE")
    unknown_remote = q_base.filter(status__icontains="Unknown error while making a remote service call:")
    failed_establish = q_base.filter(status__icontains="Failed to establish a connection")
    aborted = q_base.filter(status__icontains="Connection aborted.")
    invalid_grant = q_base.filter(status__icontains="invalid_grant")
    
    # force the canonical status messages
    doctype.update(status=UNEXPECTED_SERVICE_RESPONSE)
    unknown_remote.update(status=UNKNOWN_REMOTE_ERROR)
    failed_establish.update(status=FAILED_TO_ESTABLISH_CONNECTION)
    aborted.update(status=CONNECTION_ABORTED)
    invalid_grant.update(status=ACCOUNT_NOT_FOUND)


class Migration(migrations.Migration):
    
    dependencies = [
        ('database', '0127_study_end_date_study_manually_stopped'),
    ]
    
    operations = [
        migrations.RunPython(clean_archived_events, reverse_code=migrations.RunPython.noop),
    ]
