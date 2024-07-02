import json
from collections import defaultdict
from datetime import date, datetime, timedelta, tzinfo
from typing import Any, Dict, List, Optional, Tuple, Union

from django.shortcuts import get_object_or_404, render
from django.utils.timezone import make_aware

from authentication.admin_authentication import authenticate_researcher_study_access
from constants.common_constants import API_DATE_FORMAT, EARLIEST_POSSIBLE_DATA_DATETIME
from constants.dashboard_constants import COMPLETE_DATA_STREAM_DICT, DASHBOARD_DATA_STREAMS
from database.dashboard_models import DashboardColorSetting, DashboardGradient, DashboardInflection
from database.data_access_models import ChunkRegistry
from database.study_models import Study
from database.user_models_participant import Participant
from libs.internal_types import ParticipantQuerySet, ResearcherRequest
from middleware.abort_middleware import abort
from pages.admin_pages import conditionally_display_study_status_warnings


DATETIME_FORMAT_ERROR = f"Dates and times provided to this endpoint must be formatted like this: " \
                        f"2010-11-22 ({API_DATE_FORMAT})"


@authenticate_researcher_study_access
def dashboard_page(request: ResearcherRequest, study_id: int):
    """ information for the general dashboard view for a study """
    study = get_object_or_404(Study, pk=study_id)
    participants = list(Participant.objects.filter(study=study_id).values_list("patient_id", flat=True))
    conditionally_display_study_status_warnings(request, study)
    return render(
        request,
        'dashboard/dashboard.html',
        context=dict(
            study=study,
            participants=participants,
            study_id=study_id,
            data_stream_dict=COMPLETE_DATA_STREAM_DICT,
            page_location='dashboard_landing',
        )
    )


@authenticate_researcher_study_access
def get_data_for_dashboard_datastream_display(
    request: ResearcherRequest, study_id: int, data_stream: str
):
    """ Parses information for the data stream dashboard view GET and POST requests left the post
    and get requests in the same function because the body of the get request relies on the
    variables set in the post request if a post request is sent --thus if a post request is sent
    we don't want all of the get request running. """
    study = Study.objects.get(pk=study_id)  # already checked in decorator
    
    # general data fetching
    participants = Participant.objects.filter(study=study).order_by("patient_id")
    data_exists, first_day, last_day, unique_dates, byte_streams = parse_data_streams(
        request, study, data_stream, participants
    )
    if first_day is None or not data_exists:
        next_url = past_url = ""
    else:
        start, end = extract_date_args_from_request(request, study.timezone)
        next_url, past_url = create_next_past_urls(first_day, last_day, start=start, end=end)
    
    show_color, color_low_range, color_high_range, all_flags_list = handle_filters(
        request, study, data_stream
    )
    
    return render(
        request,
        'dashboard/data_stream_dashboard.html',
        context=dict(
            study=study,
            data_stream=COMPLETE_DATA_STREAM_DICT.get(data_stream),
            times=unique_dates,
            byte_streams=byte_streams,
            base_next_url=next_url,
            base_past_url=past_url,
            study_id=study_id,
            data_stream_dict=COMPLETE_DATA_STREAM_DICT,
            color_low_range=color_low_range,
            color_high_range=color_high_range,
            first_day=first_day,
            last_day=last_day,
            show_color=show_color,
            all_flags_list=all_flags_list,
            page_location='dashboard_data',
        )
    )


def parse_data_streams(
    request: ResearcherRequest, study: Study, data_stream: str, participant_objects: ParticipantQuerySet
):
    start, end = extract_date_args_from_request(request, study.timezone)
    first_day, last_day = dashboard_chunkregistry_date_query(study, data_stream)
    data_exists = False
    unique_dates = []
    byte_streams = {}
    if first_day is not None:
        stream_data = dashboard_chunkregistry_query(participant_objects, data_stream=data_stream)
        unique_dates, _, _ = get_unique_dates(start, end, first_day, last_day)
        
        # get the byte streams per date for each patient for a specific data stream for those dates
        byte_streams = dict(
            (participant.patient_id,
                [get_bytes_date_match(stream_data[participant.patient_id], date) for date in unique_dates])
            for participant in participant_objects
        )
        # check if there is data to display
        data_exists = len([data for patient in byte_streams for data in byte_streams[patient] if data is not None]) > 0
    
    return data_exists, first_day, last_day, unique_dates, byte_streams


@authenticate_researcher_study_access
def dashboard_participant_page(request: ResearcherRequest, study_id, patient_id):
    """ Parses data to be displayed for the singular participant dashboard view """
    study = get_object_or_404(Study, pk=study_id)
    participant = get_object_or_404(Participant, patient_id=patient_id, study_id=study_id)
    
    # query is optimized for bulk participants, so this is a little weird
    chunk_data = dashboard_chunkregistry_query(participant)
    chunks = chunk_data[participant.patient_id] if participant.patient_id in chunk_data else {}
    
    # ----------------- dates for bytes data streams -----------------------
    if chunks:
        start, end = extract_date_args_from_request(request, study.timezone)
        first_day, last_day = dashboard_chunkregistry_date_query(study, participant=participant)
        unique_dates, first_date_data_entry, last_date_data_entry = get_unique_dates(
            start, end, first_day, last_day, chunks
        )
        next_url, past_url = create_next_past_urls(
            first_date_data_entry, last_date_data_entry, start=start, end=end
        )
        byte_streams: Dict[str, List[int]] = {
            stream: [get_bytes_data_stream_match(chunks, date, stream) for date in unique_dates]
                for stream in DASHBOARD_DATA_STREAMS
        }
    else:
        last_date_data_entry = first_date_data_entry = None
        byte_streams = {}
        unique_dates = []
        next_url = past_url = first_date_data_entry = last_date_data_entry = ""
    
    patient_ids = list(
        Participant.objects.filter(study=study_id)
            .exclude(patient_id=patient_id).values_list("patient_id", flat=True)
    )
    return render(
        request,
        'dashboard/participant_dashboard.html',
        context=dict(
            study=study,
            patient_id=patient_id,
            participant=participant,
            times=unique_dates,
            byte_streams=byte_streams,
            next_url=next_url,
            past_url=past_url,
            patient_ids=patient_ids,
            study_id=study_id,
            first_date_data=first_date_data_entry,
            last_date_data=last_date_data_entry,
            data_stream_dict=COMPLETE_DATA_STREAM_DICT,
            page_location='dashboard_patient',
        )
    )


def get_unique_dates(start: datetime, end: datetime, first_day: date, last_day: date, chunks=None):
    """ Create a list of all the unique days in which data was recorded for this study """
    first_date_data_entry = last_date_data_entry = None
    
    if chunks:
        # chunks are sourced from dashboard_chunkregistry_query, so should be in the study timezone
        all_dates = sorted(
            chunk["time_bin"].date() for chunk in chunks if chunk["time_bin"].date() >= first_day
            # must be >= first day bc there are some point for 1970 that get filtered out bc obv are garbage
        )
        
        # create a list of all of the valid days in this study
        first_date_data_entry = all_dates[0]
        last_date_data_entry = all_dates[-1]
    
    # validate start date is before end date
    if start and end and (end.date() - start.date()).days < 0:
        temp = start
        start = end
        end = temp
    
    # unique_dates is all of the dates for the week we are showing
    if start is None:  # if start is none default to end
        end_num = min((last_day - first_day).days + 1, 7)
        unique_dates = [
            (last_day - timedelta(days=end_num - 1)) + timedelta(days=days) for days in range(end_num)
        ]
    elif end is None:
        # if end is none default to 7 days
        end_num = min((last_day - start.date()).days + 1, 7)
        unique_dates = [(start.date() + timedelta(days=date)) for date in range(end_num)]
    elif (start.date() - first_day).days < 0:
        # case: out of bounds at beginning to keep the duration the same
        end_num = (end.date() - first_day).days + 1
        unique_dates = [(first_day + timedelta(days=date)) for date in range(end_num)]
    elif (last_day - end.date()).days < 0:
        # case: out of bounds at end to keep the duration the same
        end_num = (last_day - start.date()).days + 1
        unique_dates = [(start.date() + timedelta(days=date)) for date in range(end_num)]
    else:
        # case: if they specify both start and end
        end_num = (end.date() - start.date()).days + 1
        unique_dates = [(start.date() + timedelta(days=date)) for date in range(end_num)]
    
    return unique_dates, first_date_data_entry, last_date_data_entry


def create_next_past_urls(first_day: date, last_day: date, start: datetime, end: datetime) -> Tuple[str, str]:
    """ set the URLs of the next/past pages for patient and data stream dashboard """
    # note: in the "if" cases, the dates are intentionally allowed outside the data collection date
    # range so that the duration stays the same if you page backwards instead of resetting
    # to the number currently shown
    if start and end:
        duration = (end.date() - start.date()).days
        start: date = start.date()
        end: date = end.date()
    else:
        duration = 6
        start: date = datetime.combine(last_day - timedelta(days=6), datetime.min.time()).date()
        end: date = datetime.combine(last_day, datetime.min.time()).date()
    days_duration = timedelta(days=duration + 1)
    one_day = timedelta(days=1)
    
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
        if chunk["time_bin"].date() == a_date and chunk["data_stream"] == stream
    )


def get_bytes_date_match(stream_data: List[Dict[str, datetime]], a_date: date) -> int or None:
    """ Returns byte value for correct stream based on ate. """
    return sum(
        data_point.get("bytes", 0) or 0 for data_point in stream_data
        if (data_point["time_bin"]).date() == a_date
    )


def dashboard_chunkregistry_date_query(
    study: Study, data_stream: str = None, participant: Participant = None
) -> Tuple[Optional[date], Optional[date]]:
    """ Gets the first and last days in the study excluding 1/1/1970 bc that is obviously an error
    and makes the frontend annoying to use """
    kwargs = {"study_id": study.id}
    if data_stream:
        kwargs["data_type"] = data_stream
    if participant:
        kwargs["participant"] = participant
    
    # this process as queries with .first() and .last() is slow even as size of all_time_bins grows.
    all_time_bins: List[datetime] = list(
        ChunkRegistry.objects.filter(**kwargs)
        .exclude(time_bin__lt=EARLIEST_POSSIBLE_DATA_DATETIME)
        .order_by("time_bin")
        .values_list("time_bin", flat=True)
    )
    
    # default behavior for 1 or 0 time_bins
    if len(all_time_bins) < 1:
        return None, None
    
    # and get as study timezone... which might be unnecessary
    return all_time_bins[0].astimezone(study.timezone).date(), \
           all_time_bins[-1].astimezone(study.timezone).date()


def dashboard_chunkregistry_query(
    participants: Union[ParticipantQuerySet, Participant], data_stream: str = None
):
    """ Queries ChunkRegistry based on the provided parameters and returns a list of dictionaries
    with 3 keys: bytes, data_stream, and time_bin. """
    if isinstance(participants, Participant):
        timezone: tzinfo = participants.study.timezone
        kwargs = {"participant": participants}
    else:
        timezone: tzinfo = participants.first().study.timezone
        kwargs = {"participant_id__in": participants}
    
    if data_stream:
        kwargs["data_type"] = data_stream
    
    # rename the data_type and file_size fields in the db query itself for speed
    chunks = ChunkRegistry.objects.filter(**kwargs).extra(
        select={'data_stream': 'data_type', 'bytes': 'file_size'}
    ).values("participant__patient_id", "bytes", "data_stream", "time_bin")
    
    patient_id_to_datapoints = defaultdict(list)
    for chunk in chunks:
        chunk["time_bin"] = chunk["time_bin"].astimezone(timezone)
        patient_id_to_datapoints[chunk.pop("participant__patient_id")].append(chunk)
    
    # populate participants with no data, values don't need to be present.
    if not isinstance(participants, Participant):
        for participant in participants:
            patient_id_to_datapoints[participant.patient_id]
    
    return dict(patient_id_to_datapoints)


def extract_date_args_from_request(request: ResearcherRequest, timezone: tzinfo) -> Tuple[Optional[datetime], Optional[datetime]]:
    """ Gets start and end arguments from GET/POST params, throws 400 on date formatting errors. """
    # "or None" handles the case of an empty string getting passed in.
    start = argument_grabber(request, "start", None) or None
    end = argument_grabber(request, "end", None) or None
    try:
        if start:
            start = make_aware(datetime.strptime(start, API_DATE_FORMAT), timezone)
        if end:
            end = make_aware(datetime.strptime(end, API_DATE_FORMAT), timezone)
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
