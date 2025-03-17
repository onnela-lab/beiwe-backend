# FILES IN UTILS SHOULD HAVE SPARSE IMPORTS SO THAT THEY CAN BE USED ANYWHERE.
# IF YOU ARE IMPORTING FROM A DATABASE MODEL YOU SHOULD PLACE IT ELSEWHERE. (ANNOTATION IMPORTS ARE OK)

from datetime import date, datetime, timedelta, tzinfo
from typing import Any, Generator, List, Union

from dateutil.tz import gettz
from django.utils import timezone
from django.utils.timezone import make_aware

from constants.common_constants import LEGIBLE_DT_FORMAT


date_or_time = Union[date, datetime]


def legible_time(time: date_or_time) -> str:
    """ Returns a legible string representation of a date or datetime including timezone. """
    # its just iso date, iso time but with a space instead of a T, and then the tz name in parens.
    # e.g. 2020-01-31 07:30:04 (America/New_York)
    return time.strftime(LEGIBLE_DT_FORMAT)


def daterange(
    start: datetime, stop: datetime, step: timedelta = timedelta(days=1), inclusive: bool = False
) -> Generator[datetime, Any, None]:
    """ Generator yielding day-separated datetimes between start and stop. """
    # source: https://stackoverflow.com/a/1060376/1940450
    if step.days > 0:
        while start < stop:
            yield start
            start = start + step
            # not +=! don't modify object passed in if it's mutable
            # since this function is not restricted to
            # only types from datetime module
    elif step.days < 0:
        while start > stop:
            yield start
            start = start + step
    if inclusive and start == stop:
        yield start


def date_list(start: date_or_time, step: timedelta, count: int) -> List[date_or_time]:
    """ less complex than daterange, provides a simple list starting on the start time and going for
    a count of steps. Length of output list is equal to count. """
    dates = [start]
    for _ in range(count - 1):
        dates.append(dates[-1] + step)
    return dates


def datetime_to_list(datetime_obj: Union[date, datetime]) -> List[int]:
    """ Takes in a date or datetime, returns a list of datetime components. """
    datetime_component_list = [datetime_obj.year, datetime_obj.month, datetime_obj.day]
    if isinstance(datetime_obj, datetime):
        datetime_component_list.extend([
            datetime_obj.hour,
            datetime_obj.minute,
            datetime_obj.second,
            datetime_obj.microsecond,
        ])
    else:
        datetime_component_list.extend([0, 0, 0, 0])
    return datetime_component_list


def date_to_start_of_day(a_date: date, tz: tzinfo):
    """ Given a date and a timezone, returns a timezone'd datetime for the start of that day. """
    if not type(a_date) is date:
        raise TypeError("date_start_of_day requires dates, datetimes must be handled manually")
    return make_aware(datetime.combine(a_date, datetime.min.time()), tz)


def date_to_end_of_day(a_date: date, tz: tzinfo):
    """ Given a date and a timezone, returns a timezone'd datetime for the end of that day. """
    if not type(a_date) is date:
        raise TypeError("date_end_of_day requires dates, datetimes must be handled manually")
    return make_aware(datetime.combine(a_date, datetime.max.time()), tz)


def get_timezone_shortcode(a_date: date, timezone_long_name: str|tzinfo) -> str:
    """ Create datetime of the END OF THE DAY of the target date and get the timezone abbreviation.
    These shortnames provide information about daylight savings, if any, in that timezone, which is
    desireable for researchers.  We always want the end of the day because then the timezone will be
    set to the timezone that _humans were paying attention to_ for that day.
    
    WARNING: modifying this code is extremely error prone, this method is stable and correct. If you
    use pytz tools resulting time will probably be wrong, specifically 4 minutes off for eastern
    time. This the string returned will be the correct abbreviation in the local time, e.g. daylight
    savings will correctly be EDT or EST for eastern time. """
    if not type(a_date) is date:
        raise TypeError("get_timezone_shortcode requires dates, datetimes must be handled manually")
    return make_aware(datetime.combine(a_date, datetime.max.time()), timezone=timezone_long_name).tzname()  # type: ignore


def date_is_in_the_past(end_date: datetime, timezone_name: str):
    """ Returns True if the date (and timezone) are in the past. """
    if end_date is None:
        return False
    tz = gettz(timezone_name)
    
    # actual_end is the start of the day after the end date
    actual_end = (
        datetime(end_date.year, end_date.month, end_date.day, tzinfo=tz)
        + timedelta(days=1)
    )
    return actual_end < timezone.now()
