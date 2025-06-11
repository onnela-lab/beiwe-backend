from collections import defaultdict
from datetime import date, datetime, timedelta, tzinfo
from typing import Any

from django.db.models.expressions import ExpressionWrapper
from django.db.models.fields import BooleanField
from django.db.models.functions.text import Lower
from django.db.models.query import QuerySet
from django.db.models.query_utils import Q
from django.utils import timezone
from django.utils.functional import Promise

from constants.common_constants import API_DATE_FORMAT, LEGIBLE_DT_FORMAT
from constants.user_constants import (BASE_TABLE_FIELD_NAMES, EXTRA_TABLE_FIELDS,
    PARTICIPANT_STATUS_QUERY_FIELDS)
from database.models import dbt
from database.study_models import Study
from database.user_models_participant import Participant


INCONCEIVABLY_HUGE_NUMBER = 2**64  # literally what it says it is, don't clutter in constants.
DT_FMT = API_DATE_FORMAT


# needs to be a trivially json-serializable type if using orjson....
# Promise class is a hack that works for the Django JSONResponse.
class SecondarySort(Promise):
    """
    Will sort towards the end of a list when compared in the python .sort() method.
    `value` is the key to use when encountering other SecondarySort objects.
    `sort_reverse` is a boolean that determines whether the sort is reversed (e.g. setting to True
        will force towards start of list).
    Serialized value is hard coded as an empty string.
    """
    
    def __init__(self, value: Any, sort_reverse: bool):
        self.value = value
        self.sort_reverse = sort_reverse
    
    def __lt__(self, other) -> bool:
        if isinstance(other, SecondarySort):
            return not ret if (ret:= self.value.__lt__(other.value)) and self.sort_reverse else ret  # :3
        return self.sort_reverse
    
    def __gt__(self, other) -> bool:
        return not self.__lt__(other)
    
    def __repr__(self):
        return f"SecondarySort<\"{self.value}\">"
    
    def __str__(self):
        return ""


def to_date_tz_str(dt: datetime, tz: tzinfo) -> str:
    """ We need a code shortener to convert a datetime to DATE in a timezone for legibility reasons. """
    return dt.astimezone(tz).strftime(API_DATE_FORMAT)


## Real code


def reference_field_and_interventions(study: Study) -> tuple[list[str], list[str]]:
    """ Returns the field and intervention names in the study, ordered by name. """
    # we need a reference list of all field and intervention names names ordered to match the
    # ordering on the rendering page. Order is defined as lowercase alphanumerical throughout.
    field_names_ordered = list(
        study.fields.values_list("field_name", flat=True).order_by(Lower('field_name'))
    )
    intervention_names_ordered = list(
        study.interventions.values_list("name", flat=True).order_by(Lower('name'))
    )
    return field_names_ordered, intervention_names_ordered


def get_table_columns(study: Study, frontend: bool) -> list[str]:
    """ Extended list of field names for the greater participant table. """
    field_names, intervention_names = reference_field_and_interventions(study)
    base_table_fields = list(BASE_TABLE_FIELD_NAMES.values())
    
    # divergence due to ordering of columns/a duplicate column on the page vs on the backend.
    if frontend:
        base_table_fields.insert(1, "First Register")
    return base_table_fields + intervention_names + field_names + list(EXTRA_TABLE_FIELDS.values())


def common_data_extraction_for_apis(study: Study, for_json: bool) -> list[list[str]]:
    # total_participants = Participant.objects.filter(study_id=study_id).count()
    table_data = get_values_for_participants_table(
        study=study,
        start=0,
        length=INCONCEIVABLY_HUGE_NUMBER,  # this is only used in a slice, we want everything
        sort_column_index=1,               # default sort by patient_id
        sort_desc_order=False,
        contains_string="",
        frontend=False,
    )
    # mutates table_data in place
    zip_extra_fields_into_participant_table_data(table_data, study, for_json=for_json)
    return table_data


def determine_registered_status(
    # these parameters are all present and explicit so that we can have a meta test for them
    now: datetime,
    registered: bool,
    permanently_retired: bool,
    last_upload: datetime|None,
    last_get_latest_surveys: datetime|None,
    last_set_password: datetime|None,
    last_set_fcm_token: datetime|None,
    last_get_latest_device_settings: datetime|None,
    last_register_user: datetime|None,
    last_heartbeat_checkin: datetime|None,
) -> str:
    """ Provides a very simple string for whether this participant is active or inactive. """
    # p.registered is a boolean, it is only present when there is no device id attached to the
    # participant, which only occurs if the participant has never registered, of if someone clicked
    # the clear device id button on the participant page.
    if not registered:
        return "Not Registered"
    
    if permanently_retired:
        return "Permanently Retired"
    
    # get a list of all the tracking timestamps
    all_the_times = [
        some_timestamp for some_timestamp in (
            last_upload,
            last_get_latest_surveys,
            last_set_password,
            last_set_fcm_token,
            last_get_latest_device_settings,
            last_register_user,
            last_heartbeat_checkin,
        ) if some_timestamp
    ]
    now = timezone.now()
    # each of the following sections will only be visible if the participant has done something
    # MORE RECENT than the time specified.
    # The values need to be alphanumerically ordered, so that sorting works on the webpage
    five_minutes_ago = now - timedelta(minutes=5)
    if any(t > five_minutes_ago for t in all_the_times):
        return "Active (just now)"
    
    one_hour_ago = now - timedelta(hours=1)
    if any(t > one_hour_ago for t in all_the_times):
        return "Active (last hour)"
    
    one_day_ago = now - timedelta(days=1)
    if any(t > one_day_ago for t in all_the_times):
        return "Active (past day)"
    
    one_week_ago = now - timedelta(days=7)
    if any(t > one_week_ago for t in all_the_times):
        return "Active (past week)"
    
    return "Inactive"


def get_interventions_and_fields(q: dbt.ParticipantQS) -> tuple[dict[int, dict[str, str]], dict[int, dict[str, date]]]:
    """ intervention dates and fields have a many-to-one relationship with participants, which means
    we need to do it as a single query (or else deal with some very gross autofilled code that I'm
    not sure populates None values in a way that we desire), from which we create a lookup dict to
    then find them later. """
    # we need the fields and intervention values, organized per-participant, by name.
    interventions_lookup = defaultdict(dict)
    fields_lookup = defaultdict(dict)
    query = q.values_list(
        "id", "intervention_dates__intervention__name", "intervention_dates__date",
        "field_values__field__field_name", "field_values__value"
    )
    
    # can you have an intervention date and a field date of the same name? probably.
    for p_id, int_name, int_date, field_name, field_value in query:
        interventions_lookup[p_id][int_name] = int_date
        fields_lookup[p_id][field_name] = field_value
    
    return dict(fields_lookup), dict(interventions_lookup)


def filtered_participants(study: Study, contains_string: str) -> QuerySet:
    """ Searches for participants with lowercase matches on os_type and patient_id, excludes deleted participants. """
    return Participant.objects.filter(study_id=study.id) \
            .filter(Q(patient_id__icontains=contains_string)|Q(os_type__icontains=contains_string)) \
            .exclude(deleted=True)


def get_values_for_participants_table(
    study: Study,
    start: int,
    length: int,
    sort_column_index: int,
    sort_desc_order: bool,
    contains_string: str,
    frontend: bool,
)-> list[list[str]]:
    """ Logic to get paginated information of the participant list on a study.
    
    This code used to be even worse - e.g. it committed the unforgivable sin of trying to speed up
    complex query logic with prefetch_related. It has been rewritten in ugly but performant and
    comprehensible values_list code that emits a total of 4 queries. This is literally a hundred
    times faster, even though it always has to pull in all the study participants.
    
    (This code has to be fast because it populates values on a page)
    
    Example extremely simple study output, no fields or interventions, no sorting or filtering
    parameters were applied:
    [['2021-12-09', <frontend only: "2021-12-09"> '1f9qb91f', 'Inactive', 'ANDROID'],
    ['2021-03-18', <frontend only: "2021-03-18"> 'bnhyxqey', 'Inactive', 'ANDROID'],
    ['2022-09-27', <frontend only: "2022-09-27"> 'c3b7mk7j', 'Inactive', 'IOS'],
    ['2021-12-09', <frontend only: "2021-12-09"> 'e1yjh259', 'Inactive', 'IOS'],
    ['2022-06-23', <frontend only: "2022-06-23"> 'ksg8clpo', 'Inactive', 'IOS'],
    ['2018-04-12', <frontend only: "2018-04-12"> 'prx7ap5x', 'Inactive', 'ANDROID'],
    ['2018-04-12', <frontend only: "2018-04-12"> 'whr8nx5b', 'Inactive', 'IOS']]
    """
    # ~ is the NOT operator - this might or might not speed up the query, whatever.
    HAS_NO_DEVICE_ID = ExpressionWrapper(~Q(device_id=''), output_field=BooleanField())
    field_names_ordered, intervention_names_ordered = reference_field_and_interventions(study)
    
    # set up the big participant query and get our lookup dicts of field values and interventions
    query = filtered_participants(study, contains_string).annotate(registered=HAS_NO_DEVICE_ID).order_by("patient_id")
    fields_lookup, intrvntns_lookup = get_interventions_and_fields(query)
    
    tz = study.timezone  # localize appropriately
    now = timezone.now()  # set time for determining status
    REGISTERED_STATUS_COLUMN = 3 if frontend else 2  # shift column index in frontend mode...
    all_participants_data = []
    
    for (
        p_pk, created_on, f_register, patient_id, registered, os_type, l_upload,
        l_get_latest_surveys, l_set_password, l_set_fcm_token, l_get_latest_device_settings,
        l_register_user, retired, l_heartbeat
    ) in query.values_list(*PARTICIPANT_STATUS_QUERY_FIELDS):
        participant_values = [to_date_tz_str(created_on, tz), patient_id, registered, os_type]
        
        if frontend:  # webpage gets first_register inserted second; it's an extra field otherwise.
            participant_values.insert(1, "" if f_register is None else to_date_tz_str(f_register, tz))
        
        # We can't trivially optimize this out because we need to be able to sort across all study
        # participants on the status column. It probably is possible to grab the lowest value of all
        # the timestamps inside the query, and then order_by on that inside the query... but have to
        # filter empty StudyFields with Nones in there somehow too, and there were comments about
        # encountering a django bug.
        participant_values[REGISTERED_STATUS_COLUMN] = determine_registered_status(
            now, registered, retired, l_upload, l_get_latest_surveys, l_set_password,
            l_set_fcm_token, l_get_latest_device_settings, l_register_user, l_heartbeat
        )
        
        # class intervention dates can be None,field values may not be present - extract, append.
        for name in intervention_names_ordered:
            participant_values.append(dat.isoformat() if (dat:= intrvntns_lookup[p_pk][name]) else "")
        for field_name in field_names_ordered:
            participant_values.append(fields_lookup[p_pk].get(field_name, ""))
        # print("\n", participant_values, "\n")
        
        all_participants_data.append(participant_values)
    
    # guarantees: presorted by patient_id, sort() itself is _stable_, all rows have same number of
    # columns, all values are non-None sortable strings.
    if frontend:  # make the frontend behave well using our SecondarySort class
        for row in all_participants_data:
            if row[sort_column_index] == "":  # on the webpage column 0in2 is the participant
                row[sort_column_index] = SecondarySort(row[2], sort_desc_order)
    
    all_participants_data.sort(key=lambda row: row[sort_column_index], reverse=sort_desc_order)
    return all_participants_data[start:start + length]


def zip_extra_fields_into_participant_table_data(
    table_data: list[list[str|datetime|None]], study: Study, for_json: bool
) -> None:
    """ Grabs the extra fields for the participants and adds them to the table. Zip like the Python
    builtin function.  """
    extra_field_data: list[tuple[datetime|None]]
    new_fields: list[str|datetime|None]
    field_values: tuple[str|datetime|None]
    patient_ids: list[str]
    
    extra_field_names = list(EXTRA_TABLE_FIELDS.keys())
    extra_field_names.append("unknown_timezone")  # not present in output
    
    # for safety: by strictly filtering the on the patient ids in case someone changes something in
    # the table logic. This Almost Definitely slows down the query by forcing a potentially huge
    # blob into the query but not a performance critical endpoint
    tz = study.timezone
    
    json_none_slug = None if for_json else "None"
    
    # the second column of a participant table is the patient id (e.g. frontend = False)
    patient_ids = [row[1] for row in table_data]  # type: ignore[assignment]
    
    extra_field_data = list(
        Participant.objects.filter(study_id=study.id, patient_id__in=patient_ids)
            .order_by(Lower('patient_id'))  # standard ordering for this "page"
            .values_list(*extra_field_names)  # at least we have SOME knowable order...
    )
    
    # Yuck we need to convert the nested datetime objects to strings, and None to "None".
    # The values here are the database fields from EXTRA_TABLE_FIELDS (duh)
    for row_number, field_values in enumerate(extra_field_data):
        # this is too hard to debug or read if we try to compact this into a list comprehension
        new_fields = []
        for i, some_value in enumerate(field_values[:-1]):
            
            if isinstance(some_value, str):  # Bool and st are easy
                new_fields.append(some_value)
            elif isinstance(some_value, bool):
                new_fields.append("True" if some_value else "False")
            
            elif some_value is None:  # None just has to be null for json
                new_fields.append(json_none_slug)
            
            # datetime needs to be a datetime for orjson, stringified for the frontend and csv
            elif isinstance(some_value, datetime):
                some_value = some_value.astimezone(tz)  # localize
                if for_json:
                    new_fields.append(some_value)
                else:
                    new_fields.append(some_value.strftime(LEGIBLE_DT_FORMAT))
            
            # datetime subclasses date, so this must be after datetime in the if statement
            elif isinstance(some_value, date):
                new_fields.append(some_value.isoformat())
            
            else:
                raise TypeError(f"Date, datetime, string, bool, or None are supported, found {type(some_value)}.")
        
        # the last field is the unknown timezone flag, which if true means we clear the value.
        if field_values[-1]:
            new_fields[2] = json_none_slug
            
        table_data[row_number].extend(new_fields)
