import functools
from datetime import datetime, tzinfo
from typing import Callable, Dict, List, Optional, Union

from dateutil import tz
from django.http.request import HttpRequest
from django.urls.base import reverse

from constants.common_constants import (API_TIME_FORMAT_WITH_TZ, DT_24HR_N_TZ_N_SEC_W_PAREN,
    DT_24HR_W_TZ_W_SEC_N_PAREN, DT_WDL_12HR_W_TZ_N_SEC_W_PAREN, DT_WDS_12HR_N_TZ_N_SEC_N_PAREN,
    DT_WDS_12HR_W_TZ_N_SEC_W_PAREN, LINE_BREAK_DT_24HR_W_TZ_W_SEC_N_PAREN)
from constants.user_constants import ANDROID_API, IOS_API
from libs.internal_types import ParticipantRequest


def easy_url(url: str, *args, **kwargs) -> str:
    """ The django reverse function, but args and kwargs are passed through to the args and kwargs
    variables.  (Imported in the jinja templates.) """
    return reverse(url, args=args, kwargs=kwargs)


def astimezone_with_tz(dt: datetime, timezone: Union[tzinfo, str]) -> str:
    """ context processor function for converting and displaying a time with a timezone """
    timezone = tz.gettz(timezone) if isinstance(timezone, str) else timezone
    return dt.astimezone(timezone).strftime(API_TIME_FORMAT_WITH_TZ)


def null_time_format(dt: Optional[datetime], timezone: Union[tzinfo, str]) -> None:
    # don't format the time, just accept args
    return


def time_with_tz(dt: datetime) -> str:
    """ context processor function for displaying a time with a timezone """
    return dt.strftime(API_TIME_FORMAT_WITH_TZ)


def more_compact_iso_time_format(dt: datetime, timezone: Union[tzinfo, str]) -> str:
    """ output looks Tue 2024-8-25 4:31 PM """
    if dt is None:
        return ""
    timezone = tz.gettz(timezone) if isinstance(timezone, str) else timezone
    return dt.astimezone(timezone).strftime(DT_24HR_N_TZ_N_SEC_W_PAREN)


def compact_iso_time_format(dt: datetime, timezone: Union[tzinfo, str]) -> str:
    """ output looks Tue 2024-8-25 4:31:00 PM """
    if dt is None:
        return ""
    timezone = tz.gettz(timezone) if isinstance(timezone, str) else timezone
    return dt.astimezone(timezone).strftime(DT_24HR_W_TZ_W_SEC_N_PAREN)


def line_break_compact_iso_time_format(dt: datetime, timezone: Union[tzinfo, str]) -> str:
    """ output looks Tue 2024-8-25<br>4:31 PM """
    if dt is None:
        return ""
    timezone = tz.gettz(timezone) if isinstance(timezone, str) else timezone
    return dt.astimezone(timezone).strftime(LINE_BREAK_DT_24HR_W_TZ_W_SEC_N_PAREN)


def niceish_iso_time_format(dt: datetime, timezone: Union[tzinfo, str]) -> str:
    """ output looks like Tue 2024-8-25, 4:31 PM """
    if dt is None:
        return ""
    timezone = tz.gettz(timezone) if isinstance(timezone, str) else timezone
    return dt.astimezone(timezone).strftime(DT_WDS_12HR_N_TZ_N_SEC_N_PAREN)


def nice_iso_dt_format(dt: datetime, timezone: Union[tzinfo, str]) -> str:
    """ output looks like Tuesday 2024-8-25, 4:31 (PM) """
    if dt is None:
        return ""
    timezone = tz.gettz(timezone) if isinstance(timezone, str) else timezone
    return dt.astimezone(timezone).strftime(DT_WDS_12HR_W_TZ_N_SEC_W_PAREN)


def fancy_dt_format_with_tz(dt: Optional[datetime], timezone: Union[tzinfo, str]) -> str:
    """ output looks like Tuesday Aug 25, 2020, 4:31 PM (EST) """
    if dt is None:
        return ""
    # getting that timezone shortname is odd because it actually depends on the time of the event
    timezone = tz.gettz(timezone) if isinstance(timezone, str) else timezone
    return dt.astimezone(timezone).strftime(DT_WDL_12HR_W_TZ_N_SEC_W_PAREN)


def list_of_checkbox_strings_to_booleans(list_checkbox_params: List[str], dict_all_params: Dict) -> None:
    """ Takes a list of strings that are to be processed as checkboxes on a post parameter,
    (checkboxes supply some arbitrary value in a post if they are checked, and no value at all if
    they are not checked.), and a dict of parameters and their values to update.
    Returns a dictionary with modified/added values containing appropriate booleans. """
    for param in list_checkbox_params:
        if param not in dict_all_params:
            dict_all_params[param] = False
        else:
            dict_all_params[param] = True


def list_of_intlike_strings_to_int(list_int_params: List[str], dict_all_params: Dict) -> None:
    for key in list_int_params:
        dict_all_params[key] = int(dict_all_params[key])


def determine_os_api(some_function: Callable) -> Callable:
    """ Add this as a decorator to a url function, under (after) the wsgi route
    decorator.  It detects if the url ends in /ios.
    This decorator provides to the function with the new variable "OS_API", which can
    then be compared against the IOS_API and ANDROID_API variables in constants.
    
    To handle any issues that arise from an undeclared keyword argument, throw
    'OS_API=""' into your url function declaration. """
    @functools.wraps(some_function)
    def provide_os_determination_and_call(*args, **kwargs):
        request: ParticipantRequest = args[0]
        assert isinstance(request, HttpRequest), \
            f"first parameter of {some_function.__name__} must be an HttpRequest, was {type(request)}."
        
        # naive, could be improved, but sufficient
        url_end = request.path[-4:].lower()
        if "ios" in url_end:
            kwargs["OS_API"] = IOS_API
        else:
            kwargs["OS_API"] = ANDROID_API
        return some_function(*args, **kwargs)
    
    return provide_os_determination_and_call
