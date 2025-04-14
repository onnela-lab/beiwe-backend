from django.utils import timezone

from database.study_models import Study
from database.system_models import GlobalSettings
from libs.schedules import repopulate_all_survey_scheduled_events


def main():
    for study in Study.objects.all():
        repopulate_all_survey_scheduled_events(study)
    
    # This flag eneables resends. This has to be set _After_ schedules are repopulated
    # because... on servers where the participants have updated the app before the server has
    # been updated will possibly be in a position where they receive a resend of every archived
    # event within that period.
    
    # - repopulating logic may generate events that were missed (because of old bugs)
    settings = GlobalSettings().singleton()
    if settings.push_notification_resend_enabled is None:
        settings.push_notification_resend_enabled = timezone.now()
        settings.save()
