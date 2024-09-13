import json
import operator
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import reduce
from typing import Any, DefaultDict, Dict, List, Optional, Tuple, Union

from django.db.models import Max, Min, Q

from constants.common_constants import API_DATE_FORMAT, EARLIEST_POSSIBLE_DATA_DATETIME
from constants.data_stream_constants import ALL_DATA_STREAMS
from constants.forest_constants import DATA_QUANTITY_FIELD_MAP, DATA_QUANTITY_FIELD_NAMES
from database.dashboard_models import DashboardColorSetting, DashboardGradient, DashboardInflection
from database.forest_models import SummaryStatisticDaily
from database.study_models import Study
from database.user_models_participant import Participant
from libs.internal_types import ParticipantQuerySet, ResearcherRequest
from middleware.abort_middleware import abort


DATETIME_FORMAT_ERROR = 'Dates and times provided to this endpoint must be formatted like this: "2010-11-22"'


def parse_data_streams(
    request: ResearcherRequest, study: Study, data_stream: str, participant_objects: ParticipantQuerySet
):
    start, end = extract_date_args_from_request(request)
    first_day, last_day = get_first_and_last_days_of_data(study, data_stream)
    data_exists = False
    unique_dates = []
    byte_streams = {}
    if first_day is not None:
        stream_data, _, _ = dashboard_data_query(participant_objects, data_stream=data_stream)
        unique_dates, _, _ = get_unique_dates(start, end, first_day, last_day)
        
        # get the byte streams per date for each patient for a specific data stream for those dates
        for participant in participant_objects:
            byte_streams[participant.patient_id] = [
                get_bytes_date_match(stream_data[participant.patient_id], date)
                for date in unique_dates
            ]
        
        # check if there is data to display
        data_exists = len([data for patient in byte_streams for data in byte_streams[patient] if data is not None]) > 0
    
    return data_exists, first_day, last_day, unique_dates, byte_streams


# FIXME document EXACTLY what this return looks like
def get_unique_dates(
    start_date: Optional[date],
    end_date: Optional[date],
    first_day: Optional[date],
    last_day: Optional[date],
    chunks: Dict[str, List[Dict[str, Union[date, str, int]]]]=None
):
    """ Create a list of all the unique days in which data was recorded for this study """
    # This code used to operate on datetimes and had timezone conversions, it was way more complex.
    
    if start_date and end_date and (end_date - start_date).days < 0:
        temp = start_date
        start_date = end_date
        end_date = temp
    
    first_date_data_entry = last_date_data_entry = None
    if chunks:
        # chunks are sourced from dashboard_data_query, so should be in the study timezone
        # must be >= first day bc there are some point for 1970 that get filtered out bc obv are garbage
        all_dates: List[date] = [chunk["date"] for chunk in chunks if chunk["date"] >= first_day]
        all_dates.sort()
        
        # create a list of all of the valid days in this study
        first_date_data_entry = all_dates[0]
        last_date_data_entry = all_dates[-1]
    
    # ensure start date is before end date, n
    
    # unique_dates is all of the dates for the week we are showing
    if start_date is None:  # if start is none default to end
        end_num = min((last_day - first_day).days + 1, 7)
        unique_dates = [
            (last_day - timedelta(days=end_num - 1)) + timedelta(days=days) for days in range(end_num)
        ]
    elif end_date is None:
        # if end is none default to 7 days
        end_num = min((last_day - start_date).days + 1, 7)
        unique_dates = [(start_date + timedelta(days=date)) for date in range(end_num)]
    elif (start_date - first_day).days < 0:
        # case: out of bounds at beginning to keep the duration the same
        end_num = (end_date - first_day).days + 1
        unique_dates = [(first_day + timedelta(days=date)) for date in range(end_num)]
    elif (last_day - end_date).days < 0:
        # case: out of bounds at end to keep the duration the same
        end_num = (last_day - start_date).days + 1
        unique_dates = [(start_date + timedelta(days=date)) for date in range(end_num)]
    else:
        # case: if they specify both start and end
        end_num = (end_date - start_date).days + 1
        unique_dates = [(start_date + timedelta(days=date)) for date in range(end_num)]
    
    return unique_dates, first_date_data_entry, last_date_data_entry


def create_next_past_urls(
    first_day: Optional[date], last_day: Optional[date], start: Optional[date], end: Optional[date],
) -> Tuple[str, str]:
    """ set the URLs of the next/past pages for patient and data stream dashboard """
    # note: in the "if" cases, the dates are intentionally allowed outside the data collection date
    # range so that the duration stays the same if you page backwards instead of resetting
    # to the number currently shown on the page.
    
    # populate duration, if either start or end are missing default to 1 week of most recent data.
    if start and end:
        duration = (end - start).days
    else:
        duration = 6
        start: date = last_day - timedelta(days=6)
        end: date = last_day
    
    days_duration = timedelta(days=duration + 1)
    one_day = timedelta(days=1)
    
    # christ this is impossible to parse
    if 0 < (start - first_day).days < duration:
        past_url = "?start=" + (start - timedelta(days=(duration + 1))).strftime(API_DATE_FORMAT) + \
                   "&end=" + (start - one_day).strftime(API_DATE_FORMAT)
    elif (start - first_day).days <= 0:
        past_url = ""
    else:
        past_url = "?start=" + (start - days_duration).strftime(API_DATE_FORMAT) + \
                   "&end=" + (start - one_day).strftime(API_DATE_FORMAT)
    
    if (last_day - days_duration) < end < (last_day - one_day):
        next_url = "?start=" + (end + one_day).strftime(API_DATE_FORMAT) + \
                   "&end=" + (end + days_duration).strftime(API_DATE_FORMAT)
    elif (last_day - end).days <= 0:
        next_url = ""
    else:
        next_url = "?start=" + (start + days_duration).strftime(API_DATE_FORMAT) \
                 + "&end=" + (end + days_duration).strftime(API_DATE_FORMAT)
    
    return next_url, past_url


def get_bytes_data_stream_match(chunks: List[Dict[str, datetime]], a_date: date, stream: str):
    """ Returns byte value for correct chunk based on data stream and type comparisons. """
    # these time_bin datetime objects should be in the appropriate timezone
    return sum(
        chunk.get("bytes", 0) or 0 for chunk in chunks
        if chunk["date"] == a_date and chunk["data_stream"] == stream
    )


# FIXME: we don't need this anymore after purging chunksregistry
def get_bytes_date_match(stream_data: List[Dict[str, datetime]], a_date: date) -> Optional[int]:
    """ Returns byte values for the declared stream based on a date. """
    return sum(
        data_point.get("bytes", 0) or 0 for data_point in stream_data
        if (data_point["date"]) == a_date
    )


# operator.or_ is the same as |, the bitwise or operator, reduce applies it to all the Q objects.
# The Q objects look like `Q(beiwe_accelerometer_bytes__isnull=False)`
# eg. filter on all streams where any data quantity field is not null
FILTER_ALL_STREAMS_WHERE_ANY_DATA_QUANTITY_FIELD_IS_NOT_NULL = reduce(
    operator.or_,
    [
        Q(**{data_stream + "__isnull": False}) for data_stream in DATA_QUANTITY_FIELD_NAMES
    ]
)


def get_first_and_last_days_of_data(
    study: Study, data_stream: Optional[str] = None, participant: Optional[Participant] = None
) -> Tuple[Optional[date], Optional[date]]:
    """ Gets the first and last days in the study, filters some junk data.
    This code used to operate on the ChunkRegisty model, that got quite slow on large servers, 
    it has been rewritted to use the SummaryStatisticDaily. This data should be the same. """
    
    if participant is None:
        kwargs = {"participant__study_id": study.id}
    else:
        kwargs = {"participant_id": participant.id}
    
    filter_args = []
    if data_stream:
        data_stream = DATA_QUANTITY_FIELD_MAP[data_stream]
        kwargs[data_stream + "__isnull"] = False  # when it is one data stream use a simple filter
    else:
        # when it is all data streams we populate this with a complex filter
        filter_args = [FILTER_ALL_STREAMS_WHERE_ANY_DATA_QUANTITY_FIELD_IS_NOT_NULL]
    
    
    # Performance notes on this query after a bunch of testing:
    # - There is a .union method, you do `q1[:1].union(q2[:1]).values_list("date", flat=True)`
    #     but it is simply slower than every other method.
    # - The really hard testing was done on ChunkRegistry, SummaryStatisticDaily is filtering
    #     a more complex query on the all data stream case, but it is much faster, it is:
    #        number_of_participants*number_of_days e.g.: 100 * 365 = 36,500
    #     vs
    #        number_of_participants*number_of_hours*number_of_streams: 100 * (24*365) * 19 = 16,644,000
    #     which is a 456x reduction in record count, plus we had to deal with a timezone conversion.
    # - !!!!! When these numbers (again, as ChunkRegistry) get very large it became faster to
    #     PULL IN ALL THE VALUES and get the min and max in Python.  I don't know why.
    
    # (min, max, Min, Max, MIN, MAX - this namespace is getting crowded)
    MIN, MAX = (
        SummaryStatisticDaily.objects.filter(*filter_args, **kwargs)
        .exclude(date__lt=EARLIEST_POSSIBLE_DATA_DATETIME)
        # .order_by("date")  # have not tested whether this affects speed
        .aggregate(min=Min("date"), max=Max("date"))
        # at this point it is now a dict like {'min': datetime, 'max': datetime}, order is guaranteed
        .values()
    )
    
    # if either one is missing we just return None, None
    if MIN is None or MAX is None:
        return None, None
    return MIN, MAX


def dashboard_data_query(
    participants: ParticipantQuerySet, data_stream: str = None
) -> Dict[str, List[Dict[str, Union[date, str, int]]]]:
    """ Queries SummaryStatistics based on the provided parameters and returns a list of dictionaries
    with 3 keys: bytes, data_stream, and time_bin. """
    
    stream_bytes_per_day = do_dashboard_summarystatistics_query(participants, data_stream)
    patient_id_to_datapoints, earliest_date, latest_date = build_participant_data(stream_bytes_per_day)
    
    # populate participants with no data, values don't need to be present.
    for participant in participants:
        if participant.patient_id not in patient_id_to_datapoints:
            patient_id_to_datapoints[participant.patient_id] = []
    
    return patient_id_to_datapoints, earliest_date, latest_date


def do_dashboard_summarystatistics_query(participants: ParticipantQuerySet, data_stream: str = None):
    """ Business logic to make the database query as fast as possible. """
    
    filter_kwargs = {"participant_id__in": participants}
    
    if data_stream:
        # the specific translated field name
        filter_kwargs[DATA_QUANTITY_FIELD_MAP[data_stream] + "__isnull"] = False
        filter_arg = Q()
        data_streams = [data_stream]
    else:
        # filters items with no data in any stream on a day, can be a substantial speedup, saw 4x.
        filter_arg = FILTER_ALL_STREAMS_WHERE_ANY_DATA_QUANTITY_FIELD_IS_NOT_NULL
        data_streams = ALL_DATA_STREAMS
    
    # Rename fields to real stream names (unclear if this is an optimization over DATA_QUANTITY_FIELD_MAP)
    # This mechanism of changing the field names is very fast, but it is also definitely very stupid 
    select_args = {stream: DATA_QUANTITY_FIELD_MAP[stream] for stream in data_streams}
    
    # including the filter FILTER_ALL_STREAMS_WHERE actually makes the query
    stream_bytes_per_day: Tuple[date, str, Optional[int]] = list(
        # trunk-ignore(bandit/B610): extra here takes a static dictionary, it is not a security risk.
        SummaryStatisticDaily.objects
        .extra(select=select_args)
        .filter(filter_arg, **filter_kwargs)
        .order_by("date")
        .values("date", "participant__patient_id", *data_streams)
    )
    return stream_bytes_per_day


def build_participant_data(
    stream_bytes_per_day: Tuple[date, str, Optional[int]]
) -> Tuple[Dict[str, List[Dict[str, Union[date, str, int]]]], date, date]:
    """ Builds a dictionary of participant ids mapped to a list of dictionaries like
        {"bytes": 5," data_stream": "gyro", "date": a_date}
    Also returns the earliest and latest dates in the data set. """
    
    # unambiguous date values
    latest_date = original_latest = date.today() - timedelta(days=1000*365)
    earliest_date = original_earliest = date.today() + timedelta(days=1000*365)
    
    # list of participant ids mapped to a list of dictionaries with keys "bytes", "data_stream", "date"
    patient_id_to_datapoints: DefaultDict[str, List[Dict[str, Union[date, str, int]]]] = defaultdict(list)
    for stream_bytes_day in stream_bytes_per_day:
        day = stream_bytes_day.pop("date")
        patient_id = stream_bytes_day.pop("participant__patient_id")
        
        earliest_date = day if day < earliest_date else earliest_date
        latest_date = day if day > latest_date else latest_date
        
        for stream_name, stream_bytes in stream_bytes_day.items():
            # value can be null with multiple data streams, we exclude those results entirely
            if stream_bytes is not None:
                patient_id_to_datapoints[patient_id].append(
                    {"data_stream": stream_name, "bytes": stream_bytes, "date": day}
                )
    
    # force Nones if the dates were still those silly values
    earliest_date = earliest_date if earliest_date is not original_earliest else None
    latest_date = latest_date if latest_date is not original_latest else None
    
    # clear the defaultdict to a normal dict
    return dict(patient_id_to_datapoints), earliest_date, latest_date


def extract_date_args_from_request(request: ResearcherRequest) -> Tuple[Optional[date], Optional[date]]:
    """ Gets start and end arguments from GET/POST params, throws 400 on date formatting errors. """
    # "or None" handles the case of an empty string getting passed in.
    start = argument_grabber(request, "start", None) or None
    end = argument_grabber(request, "end", None) or None
    try:
        if start:
            start = datetime.strptime(start, API_DATE_FORMAT).date()
        if end:
            end = datetime.strptime(end, API_DATE_FORMAT).date()
    except ValueError:
        return abort(400, DATETIME_FORMAT_ERROR)
    
    return start, end


def argument_grabber(request: ResearcherRequest, key: str, default: Any = None) -> Optional[str]:
    return request.GET.get(key, request.POST.get(key, default))


#
## Post request parameters, mostly colors and gradients
#

def extract_range_args_from_request(request: ResearcherRequest):
    """ Gets minimum and maximum arguments from GET/POST params """
    return argument_grabber(request, "color_low", None), \
           argument_grabber(request, "color_high", None), \
           argument_grabber(request, "show_color", True)


def extract_flag_args_from_request(request: ResearcherRequest):
    """ Gets minimum and maximum arguments from GET/POST params as a list """
    # parse the "all flags string" to create a dict of flags
    flags_separated = argument_grabber(request, "flags", "").split('*')
    all_flags_list = []
    for flag in flags_separated:
        if flag != "":
            flag_apart = flag.split(',')
            all_flags_list.append([flag_apart[0], int(flag_apart[1])])
    return all_flags_list


def set_default_settings_post_request(request: ResearcherRequest, study: Study, data_stream: str):
    all_flags_list = argument_grabber(request, "all_flags_list", "[]")
    color_high_range = argument_grabber(request, "color_high_range", "0")
    color_low_range = argument_grabber(request, "color_low_range", "0")
    
    # convert parameters from unicode to correct types
    # if they didn't save a gradient we don't want to save garbage
    all_flags_list = json.loads(all_flags_list)
    if color_high_range == "0" and color_low_range == "0":
        color_low_range, color_high_range = 0, 0
        bool_create_gradient = False
    else:
        bool_create_gradient = True
        color_low_range = int(json.loads(color_low_range))
        color_high_range = int(json.loads(color_high_range))
    
    # try to get a DashboardColorSetting object and check if it exists
    if DashboardColorSetting.objects.filter(data_type=data_stream, study=study).exists():
        # case: a default settings model already exists; delete the inflections associated with it
        gradient: DashboardGradient
        inflection: DashboardInflection
        settings: DashboardColorSetting = DashboardColorSetting.objects.get(
            data_type=data_stream, study=study
        )
        settings.inflections.all().delete()
        if settings.gradient_exists():
            settings.gradient.delete()
        
        if bool_create_gradient:
            # create new gradient
            gradient, _ = DashboardGradient.objects.get_or_create(dashboard_color_setting=settings)
            gradient.color_range_max = color_high_range
            gradient.color_range_min = color_low_range
            gradient.save()
        
        # create new inflections
        for flag in all_flags_list:
            # all_flags_list looks like this: [[operator, inflection_point], ...]
            inflection = DashboardInflection.objects.create(
                dashboard_color_setting=settings, operator=flag[0]
            )
            inflection.operator = flag[0]
            inflection.inflection_point = flag[1]
            inflection.save()
        settings.save()
    else:
        # this is the case if a default settings does not yet exist
        # create a new dashboard color setting in memory
        settings = DashboardColorSetting.objects.create(data_type=data_stream, study=study)
        
        # create new gradient
        if bool_create_gradient:
            gradient = DashboardGradient.objects.create(dashboard_color_setting=settings)
            gradient.color_range_max = color_high_range
            gradient.color_range_min = color_low_range
        
        # create new inflections
        for flag in all_flags_list:
            inflection = DashboardInflection.objects.create(
                dashboard_color_setting=settings, operator=flag[0]
            )
            inflection.operator = flag[0]
            inflection.inflection_point = flag[1]
        
        # save the dashboard color setting to the backend (currently is just in memory)
        settings.save()
    
    return color_low_range, color_high_range, all_flags_list


def handle_filters(request: ResearcherRequest, study: Study, data_stream: str):
    color_settings: DashboardColorSetting
    
    if request.method == "POST":
        color_low_range, color_high_range, all_flags_list =\
            set_default_settings_post_request(request, study, data_stream)
        show_color = "false" if color_low_range == 0 and color_high_range == 0 else "true"
    else:
        color_low_range, color_high_range, show_color = extract_range_args_from_request(request)
        all_flags_list = extract_flag_args_from_request(request)
    
    if DashboardColorSetting.objects.filter(data_type=data_stream, study=study).exists():
        color_settings = DashboardColorSetting.objects.get(data_type=data_stream, study=study)
        default_filters = DashboardColorSetting.get_dashboard_color_settings(color_settings)
    else:
        default_filters = ""
        color_settings = None
    
    # -------------------------------- dealing with color settings -------------------------------------------------
    # test if there are default settings saved,
    # and if there are, test if the default filters should be used or if the user has overridden them
    if default_filters != "":
        inflection_info = default_filters["inflections"]
        if all_flags_list == [] and color_high_range is None and color_low_range is None:
            # since none of the filters are set, parse default filters to pass in the default
            # settings set the values for gradient filter
            
            # backend: color_range_min, color_range_max --> frontend: color_low_range,
            # color_high_range the above is consistent throughout the back and front ends
            if color_settings.gradient_exists():
                gradient_info = default_filters["gradient"]
                color_low_range = gradient_info["color_range_min"]
                color_high_range = gradient_info["color_range_max"]
                show_color = "true"
            else:
                color_high_range, color_low_range = 0, 0
                show_color = "false"
            
            # set the values for the flag/inflection filter*s*
            # the html is expecting a list of lists for the flags [[operator, value], ... ]
            all_flags_list = [
                [flag_info["operator"], flag_info["inflection_point"]]
                for flag_info in inflection_info
            ]
    
    # change the url params from jinja t/f to python understood T/F
    show_color = True if show_color == "true" else False
    
    return show_color, color_low_range, color_high_range, all_flags_list
