from collections import defaultdict
from typing import Dict, Generator, List, Union

from django.db.models import F, Func, Sum

from constants.forest_constants import DATA_QUANTITY_FIELD_NAMES
from database.forest_models import SummaryStatisticDaily
from database.study_models import Study


# a lookup for the field names generated here (ending in "_total") to stripped down field names
# (we don't want the super nice names with title casing and spaces, this is API output)
NICE_FIELD_NAME_CONVERSION = dict(zip(
    [name.replace("beiwe_", "") + "_total" for name in DATA_QUANTITY_FIELD_NAMES],
    [name.replace("beiwe_", "") for name in DATA_QUANTITY_FIELD_NAMES],
))


def get_participant_data_upload_summary(study: Study) -> Dict[str, Dict[str, int]]:
    """ Returns per-participant-data of bytes for each summary statistic. """
    raw_data = query_for_summed_data_summaries(study)
    participant_data = sum_participant_data_query(raw_data)
    insert_missing_empty_participants(study, participant_data)
    return participant_data


def sum_participant_data_query(participant_summation_query: List[Dict[str, int]]) -> Dict[str, Dict[str, int]]:
    """ The database query can return multiple rows for each participant, it is unclear why, we 
    suspect it is something to do with internal database ordering. """
    per_participant_data = {}
    
    # sum up all the values for each participant using a defaultdict
    for participant_data in participant_summation_query:
        patient_id = participant_data.pop("patient_id")
        
        # first time encountering a participant, create a defaultdict for them
        if patient_id not in per_participant_data:
            per_participant_data[patient_id] = defaultdict(int)
        
        # for each value in the participant's data, add it to the defaultdict, transform None to 0,
        # and separate and title case the keys
        for key, value in participant_data.items():
            key = NICE_FIELD_NAME_CONVERSION[key]
            value = value if value else 0  # force numeric
            per_participant_data[patient_id][key] += value  # increment participant value
    
    # convert the defaultdicts to dictionaries for safety
    for patient_id in per_participant_data.keys():
        per_participant_data[patient_id] = dict(per_participant_data[patient_id])
    
    return per_participant_data


def insert_missing_empty_participants(study: Study, participant_data: Dict[str, Dict[str, int]]):
    """ Ensure that there is an entry for every participant in the study, even if they have no data."""
    for patient_id in study.participants.order_by("patient_id").values_list("patient_id", flat=True):
        if patient_id not in participant_data:
            # to save us from our future selves we will make these unique dictionaries
            participant_data[patient_id] = {
                k: 0 for k in NICE_FIELD_NAME_CONVERSION.values()
            }


def query_for_summed_data_summaries(study: Study) -> List[Dict[str, Union[int, str]]]:
    """ For every field name in DATA_QUANTITY_FIELD_NAMES, generate an annotated field that sums all
    values of that field for each participant. """
    
    # The reason this query magically works is due to a difference between these
    # two ways to add an annotation to a query:
    #   1) query_annotation_params[summation_field_name] = Func(field_name, function='SUM')
    #   2) query_annotation_params[summation_field_name] = Sum(field_name)
    # The first one must be used from a participant.summarystatisticdaily_set query, otherwise
    # you get a SQL "column "database_participant.patient_id" must appear in the GROUP BY clause
    #   or be used in an aggregate function" error.
    # The second one is used in the query below, but there will be duplicate grouped rows for each
    #   participant that you have to sum together afterwards in python.
    # Also, this query will miss participants that have no data quantity fields, so you have to
    #   populate those later for completeness.
    
    query_annotation_params = {"patient_id": F("participant__patient_id")}
    for field_name in DATA_QUANTITY_FIELD_NAMES:
        summation_field_name = f"{field_name.replace('beiwe_', '')}_total"
        query_annotation_params[summation_field_name] = Sum(field_name)
    
    # single query for sum of all data quantity fields
    participant_summation: Dict[str, int] = list(
        SummaryStatisticDaily.objects
        .filter(participant__study=study)
        .order_by("participant__patient_id")
        .annotate(**query_annotation_params)
        .values(*query_annotation_params.keys())
    )
    return participant_summation


def reference_summarize_data_summaries(study: Study) -> Generator[Dict[str, int], None, None]:
    """ This is a much less complex way to do the same thing as the single-query version, but it
    results in far too many database queries. Implemented as a generator, not used in production.
    
    The equivalence of these two implementations was exhaustedly tested, could not work out how
    to group the data in the single-query version ... chunks? tegether into individual "rows"
    of annotated participant fields. """
    
    # For every field name in DATA_QUANTITY_FIELD_NAMES, generate an annotated field that sums all
    # values of that field for each participant. Structure is a dict every line that looks like this:
    #  'Accelerometer Bytes Total': 1347977012
    #  'Ambient Audio Bytes Total': 0
    #  'App Log Bytes Total': 450205033
    #  'Audio Recordings Bytes Total': 53804
    #  'Bluetooth Bytes Total': 0
    #  'Calls Bytes Total': 0
    #  'Devicemotion Bytes Total': 0
    #  'Gps Bytes Total': 39812970
    #  'Gyro Bytes Total': 0
    #  'Identifiers Bytes Total': 484
    #  'Ios Log Bytes Total': 0
    #  'Magnetometer Bytes Total': 0
    #  'Power State Bytes Total': 9122
    #  'Proximity Bytes Total': 0
    #  'Reachability Bytes Total': 0
    #  'Survey Answers Bytes Total': 455
    #  'Survey Timings Bytes Total': 1191
    #  'Texts Bytes Total': 0
    #  'Wifi Bytes Total': 4762084312
    
    for participant in study.participants.all():
        query_annotation_params = {}
        for field_name in DATA_QUANTITY_FIELD_NAMES:
            summation_field_name = f"{field_name.replace('beiwe_', '')}_total"
            query_annotation_params[summation_field_name] = Func(field_name, function='SUM')
        
        # single query for sum of all data quantity fields
        participant_summation: Dict[str, int] = participant.summarystatisticdaily_set \
            .annotate(**query_annotation_params).values(*query_annotation_params.keys()).get()
        
        final_dict = {
            NICE_FIELD_NAME_CONVERSION[k]: v if v else 0 for k, v in participant_summation.items()
        }
        final_dict["Patient Id"] = participant.patient_id
        yield final_dict
