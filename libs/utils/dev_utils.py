# FILES IN UTILS SHOULD HAVE SPARSE IMPORTS SO THAT THEY CAN BE USED ANYWHERE.
# IF YOU ARE IMPORTING FROM A DATABASE MODEL YOU SHOULD PLACE IT ELSEWHERE. (ANNOTATION IMPORTS ARE OK)

import cProfile
import functools
from collections import defaultdict
from inspect import getframeinfo, stack
from math import sqrt
from os.path import relpath
from pprint import pprint
from statistics import mean, stdev
from time import perf_counter
from types import FunctionType, MethodType

from libs.utils.security_utils import generate_easy_alphanumeric_string


PROJECT_PATH = __file__.rsplit("/", 2)[0]
class DevUtilError(BaseException): pass


def profileit(func):
    def wrapper(*args, **kwargs):
        datafn = func.__name__ + "-" + generate_easy_alphanumeric_string(8) + ".profile"
        prof = cProfile.Profile()
        try:
            retval = prof.runcall(func, *args, **kwargs)
        except BaseException:
            prof.dump_stats(datafn)
            raise
        prof.dump_stats(datafn)
        
        return retval
    
    return wrapper


class TxtClr:
    BLACK = "\x1b[0m"  # "default"?
    RED = "\x1b[31m"
    YELLOW = "\x1b[33m"
    GREEN = "\x1b[32m"
    CYAN = "\x1b[36m"
    
    @classmethod
    def brightness_swap(cls):
        if cls.RED != "\x1b[31m":
            cls.RED = "\x1b[31m"
            cls.YELLOW = "\x1b[33m"
            cls.GREEN = "\x1b[32m"
            cls.CYAN = "\x1b[36m"
        else:
            cls.RED = "\x1b[91m"
            cls.YELLOW = "\x1b[93m"
            cls.GREEN = "\x1b[92m"
            cls.CYAN = "\x1b[96m"


def disambiguate_participant_survey(func):
    """ This wrapper allows a function to take any combination of (participant, survey)
    Mostly used in debugging push notifications. """
    
    from database.survey_models import Survey
    from database.user_models_participant import Participant
    
    @functools.wraps(func)
    def _disambiguate_participant_survey(*args, **kwargs):
        args = list(args)  # not initially mutable
        
        participant = args[0]  # The first parameter is positional
        survey = args[1] if len(args) >= 2 else None
        
        msg = "pass in a survey object, a survey's object_id key, a participant, or a participnt's patient_id"
        assert participant is None or isinstance(participant, (Survey, Participant, str)), msg
        assert survey is None or isinstance(survey, (Survey, str)), msg
        
        # case: (participant: None, survey: something).  we actually handle that in reverse already!
        if participant is None and survey is not None:
            participant, survey = survey, participant
        
        # allows passing in just a survey - if no survey and participant is a survey
        if not survey and isinstance(participant, Survey):
            participant, survey = survey, participant
        
        # if only a participant but its an object_id
        if isinstance(participant, str) and len(participant) == 24:
            participant, survey = survey, participant
        
        # string to participant
        if isinstance(participant, str):
            try:
                participant = Participant.objects.get(patient_id=participant)
            except Participant.DoesNotExist:
                raise TypeError(f"no matching participant for '{participant}'")
        
        # string to survey
        if isinstance(survey, str):
            if len(survey) == 24:
                try:
                    survey = Survey.objects.get(object_id=survey)
                except Survey.DoesNotExist:
                    pass
            else:
                raise TypeError(f"'{survey}' was a string, but it had the wrong length...")
        
        # reassign and/or add
        args[0] = participant
        # if we swapped survey or if there was originally a parameter we treated as a survey
        if len(args) > 1:
            args[1] = survey
        elif survey:
            args.append(survey)
        
        return func(*args, **kwargs)
    
    return _disambiguate_participant_survey


class GlobalTimeTracker:
    """
    Populate with events, prints event time summaries on deallocation.

    To avoid overhead we initialize an instance of the GlobalTimeTracker
    when the first event is added.  This way the object is only instantiated
    if there are any events.  The __del__ function is an instance method,
    so by initializing lazily we skip it entirely.
    """
    
    function_pointers = defaultdict(list)
    global_time_tracker = None
    
    @classmethod
    def add_event(
            cls, function: FunctionType or MethodType, duration: float, exception: Exception = None
    ):
        # initialize and point to new function on the first call
        cls.global_time_tracker = GlobalTimeTracker()
        
        #
        # This is the REAL add_event function
        #
        @classmethod  # this decorator ... is allowed here. Cool.
        def _add_event(
                cls: GlobalTimeTracker,  # and we can declare our own type, lol.
                function: FunctionType or MethodType,
                duration: float,
                exception: Exception = None
        ):
            if exception is None:
                cls.function_pointers[function. __name__, function].append(duration)
            else:
                cls.function_pointers[function.__name__, function, str(exception)].append(duration)
        
        # repoint, call.
        cls.add_event = _add_event
        cls.add_event(function=function, duration=duration, exception=exception)
    
    @classmethod
    def print_summary(cls):
        """ Prints a summary of runtime statistics."""
        
        for components, times in cls.function_pointers.items():
            name = components[0]
            pointer = components[1]
            exception = None if len(components) <= 2 else components[2]
            # these as variable names is easier
            name_and_exception = f"{name} {str(exception)}"
            final_name = name if not exception else name_and_exception
            
            print(
                "\n"
                f"function: {final_name}",
                "\n"
                f"calls: {len(times)}",
                "\n"
                f"total milliseconds: {sum(times)}",
                "\n"
                f"min: {min(times)}",
                f"\n"
                f"max: {max(times)}",
                f"\n"
                f"mean: {mean(times)}",
                "\n"
                f"rms: {sqrt(mean(t * t for t in times))}",
                "\n"
                f"stdev: {'xxx' if len(times) == 1 else stdev(times)}"
            )
    
    def __del__(self, *args, **kwargs):
        """ On deallocation, print a bunch of statistics. """
        self.print_summary()
    
    @staticmethod
    def track_function(some_function):
        """ wraps a function with a timer that records to a GlobalTimeTracker"""
        @functools.wraps(some_function)
        def wrapper(*args, **kwargs):
            try:
                # perf counter is in milliseconds
                t_start = perf_counter() * 1000
                ret = some_function(*args, **kwargs)
                t_end = perf_counter() * 1000
                GlobalTimeTracker.add_event(some_function, t_end - t_start)
                return ret
            except Exception as e:
                t_end = perf_counter() * 1000
                # t_start always exists unless there is a bug in perf_timer
                GlobalTimeTracker.add_event(some_function, t_end - t_start, e)
                raise
        return wrapper


def print_types(display_value=True, **kwargs):
    if display_value:
        for k, v in kwargs.items():
            print(f"TYPE INFO -- {k}: {v}, {type(v)}")
    else:
        for k, v in kwargs.items():
            print(f"TYPE INFO -- {k}: {type(v)}")


already_processed = set()


def print_entry_and_return_types(some_function):
    """ Decorator for functions (pages) that require a login, redirect to login page on failure. """
    @functools.wraps(some_function)
    def wrapper(*args, **kwargs):
        name = getframeinfo(stack()[1][0]).filename.strip(PROJECT_PATH) + ": " + some_function.__name__
        
        # args and kwargs COULD mutate
        args_dict = {i: type(v) for i, v in enumerate(args)}
        kwargs_dict = {k: type(v) for k, v in kwargs.items()}
        # don't print multiple times...
        
        # place before adding to processed
        if name in already_processed:
            try:
                return some_function(*args, **kwargs)
            except Exception:
                if args_dict:
                    print(f"args in {name} (IT ERRORED!):")
                    pprint(args_dict)
                if kwargs_dict:
                    print(f"kwargs in {name} (IT ERRORED!):")
                    pprint(kwargs_dict)
                raise
        
        already_processed.add(name)
        
        rets = some_function(*args, **kwargs)
        
        if args_dict:
            print(f"args in {name}:")
            pprint(args_dict)
        
        if kwargs_dict:
            print(f"kwargs in {name}:")
            pprint(kwargs_dict)
        
        if isinstance(rets, tuple):
            types = ", ".join(str(type(t)) for t in rets)
            print(f"return types - {name} -> ({types})")
        else:
            print(f"return type - {name} -> {type(rets)}")
        return rets
    
    return wrapper


class timer_class():
    """ This is a simple class that is at the heart of the p() function declared below.
        This class consists of a datetime timer and a single function to access and advance it. """
    
    def __init__(self):
        self.timestamp = 0
    
    def set_timer(self, timestamp):
        self.timestamp = timestamp


# we use a defaultdict of timers to allow an arbitrary number of such timers.
timers = defaultdict(timer_class)


def pwrap(a_function):
    @functools.wraps(a_function)
    def wrapper(*args, **kwargs):
        p(a_function.__name__, quiet=True, name="Start " + a_function.__name__)
        ret = a_function(*args, **kwargs)
        p(a_function.__name__, name="Finished " + a_function.__name__)
        return ret
    
    return wrapper


def p(timer_label=0, caller_stack_location=1, quiet=False, name=None):
    """ Handy little function that prints the file name line number it was called on and the
        amount of time since the function was last called.
        If you provide a label (anything with a string representation) that will be printed
        along with the time information.
    
    Examples:
         No parameters (source line numbers present for clarity):
            [app.py:65] p()
            [app.py:66] sleep(0.403)
            [app.py:67] p()
         This example's output:
            app.py:65 -- 0 -- profiling start...
            app.py:67 -- 0 -- 0.405514
        
         The second statement shows that it took just over the 0.403 time of the sleep statement
         to process between the two p calls.
        
         With parameters (source line numbers present for clarity):
             [app.py:65] p()
             [app.py:66] sleep(0.403)
             [app.py:67] p(1)
             [app.py:68] sleep(0.321)
             [app.py:69] p(1)
             [app.py:70] p()
         This example's output:
             app.py:65 -- 0 -- profiling start...
             app.py:67 -- 1 -- profiling start...
             app.py:69 -- 1 -- 0.32679
             app.py:70 -- 0 -- 0.731086
         Note that the labels are different for the middle two print statements.
         In this way you can interleave timers with any label you want and time arbitrary,
         overlapping subsections of code.  In this case I have two timers, one timed the
         whole process and one timed only the second timer.
    """
    timestamp = perf_counter()
    timer_object = timers[timer_label]
    
    # Change the print statement by shifting the stack location where the caller name is sourced from.
    # Only very occasionally useful, values other than 1 and 2 are probably never useful.
    caller = getframeinfo(stack()[caller_stack_location][0])
    
    # Sometimes you need to make a name pop out.
    if name:
        timer_label = name
    
    print("%s:%.f -- %s --" % (relpath(caller.filename), caller.lineno, timer_label), end=" ")
    # the first call to get_timer results in a zero elapsed time, so we can skip it.
    if timer_object.timestamp == 0 or quiet:
        print("timer start...")
    else:
        print('%.10f' % (timestamp - timer_object.timestamp))
    # and at the very end we need to set the timer to the end of our actions (we have print
    # statements, which are slow)
    timer_object.set_timer(perf_counter())
