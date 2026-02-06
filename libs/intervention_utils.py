from collections import defaultdict
from datetime import date

import orjson
from django.db.models.aggregates import Count

from database.schedule_models import Intervention, InterventionDate, RelativeSchedule
from database.study_models import Study
from database.user_models_participant import Participant, ParticipantFieldValue


DictOfStrStr = dict[str, str]

ENROLLMENT_DATE_INTERVENTION_NAME = "Enrollment Date"


def create_enrollment_date_intervention_for_study(study: Study) -> Intervention:
    """
    Creates the 'Enrollment Date' intervention for a study.
    Called when a new study is created.

    Uses get_or_create to handle edge cases (e.g., study duplication
    where the intervention might already exist from the copied study).
    """
    intervention, _ = Intervention.objects.get_or_create(
        study=study,
        name=ENROLLMENT_DATE_INTERVENTION_NAME
    )
    return intervention


def set_enrollment_date(participant: Participant) -> None:
    """
    Sets the enrollment date for a participant to today's date in the study's timezone.
    Called when a participant first registers via the mobile app.

    Only sets the date if the "Enrollment Date" intervention exists for the study.
    Does nothing for existing studies that don't have this intervention.
    """
    from django.utils import timezone

    intervention = Intervention.objects.filter(
        study=participant.study,
        name=ENROLLMENT_DATE_INTERVENTION_NAME
    ).first()

    if intervention is None:
        # Intervention doesn't exist for this study (e.g., existing study
        # created before this feature). Do nothing.
        return

    # Get the current date in the study's timezone
    study_timezone = participant.study.timezone
    enrollment_date = timezone.now().astimezone(study_timezone).date()

    InterventionDate.objects.update_or_create(
        participant=participant,
        intervention=intervention,
        defaults={'date': enrollment_date}
    )


def intervention_survey_data(study: Study) -> dict[str, dict[str, DictOfStrStr]]:
    # this was manually tested to cover multiple interventions per survey, and multiple surveys per intervention
    intervention_dates_data = (
        InterventionDate.objects.filter(
            participant__in=study.participants.all()
        ).values_list("participant__patient_id", "intervention__name", "date")
    )
    
    intervention_name_to_survey_id = dict(
        RelativeSchedule.objects.filter(intervention__in=study.interventions.all()
                                       ).values_list("intervention__name", "survey__object_id")
    )
    
    intervention_date: date
    final_data = defaultdict(lambda: defaultdict(dict))
    # there may be participants with no intervention dates, and there may be deleted interventions?
    for patient_id, intervention_name, intervention_date in intervention_dates_data:
        try:
            survey_object_id = intervention_name_to_survey_id[intervention_name]
        except KeyError:
            continue
        if intervention_date:
            intervention_date = intervention_date.isoformat()
        final_data[patient_id][survey_object_id][intervention_name] = intervention_date
    
    # convert defaultdicts to regular dicts so the data can be jsonified.
    final_data = dict(final_data)
    for k1 in final_data:
        final_data[k1] = dict(final_data[k1])
    return final_data


def survey_history_export(study: Study) -> bytes:
    """ Survey history includes the most recent version of the survey. """
    survey_archives = defaultdict(list)
    # get every survey archive for every survey in a study.
    # There isn't enough study data to bother to further optimize this down to one query
    # profiled: longest time on production was 0.15 seconds.
    for survey in study.surveys.all():
        query_list = list(
            survey.archives.order_by("archive_start").values("archive_start", "content")
        )
        # we unpack the json of the survey object here.
        for survey_archive_dict in query_list:
            survey_archive_dict["survey_json"] = orjson.loads(survey_archive_dict.pop("content"))
        survey_archives[survey.object_id].extend(query_list)
    
    return orjson.dumps(survey_archives)


# FIXME: this issues too many queries.
def add_fields_and_interventions(participant: Participant, study: Study):
    """ Creates empty ParticipantFieldValue and InterventionDate objects for newly created
     participants, doesn't affect existing instances. """
    for field in study.fields.all():
        ParticipantFieldValue.objects.get_or_create(participant=participant, field=field)
    for intervention in study.interventions.all():
        InterventionDate.objects.get_or_create(participant=participant, intervention=intervention)


def correct_bad_interventions(study: Study):
    """ Identify participants with incorrect intervention date counts and attempt to correct them.
    Only handles the case where the number of intervention dates is less than the expected number of
    interventions. """
    intervention_count = study.interventions.all().count()
    bad_participants = study.participants.annotate(
        count=Count('intervention_dates')).exclude(count=intervention_count
    )
    for participant in bad_participants:
        add_fields_and_interventions(participant, study)
