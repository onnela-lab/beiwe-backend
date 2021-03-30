import datetime


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
