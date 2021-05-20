import datetime
from typing import List


def daterange(start, stop, step=datetime.timedelta(days=1), inclusive=False):
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


def datetime_to_list(datetime_object: datetime.date) -> List[int]:
    """
    Takes in a `datetime.date` or `datetime.datetime` and returns a list of datetime components.
    """
    datetime_component_list = [datetime_object.year, datetime_object.month, datetime_object.day]
    if isinstance(datetime_object, datetime.datetime):
        datetime_component_list.extend([
            datetime_object.hour,
            datetime_object.minute,
            datetime_object.second,
            datetime_object.microsecond,
        ])
    else:
        datetime_component_list.extend([0, 0, 0, 0])
    return datetime_component_list
