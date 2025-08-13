from __future__ import annotations

# WARNING: THIS FILE IS IMPORTED IN THE DJANGO CONF FILE. BE CAREFUL WITH IMPORTS.
from threading import Thread
from time import sleep
from typing import Callable

import sentry_sdk
from cronutils.error_handler import ErrorSentry, null_error_handler
from django.utils import timezone
from sentry_sdk import capture_message, set_tag
from sentry_sdk.transport import HttpTransport

from config.settings import SENTRY_DATA_PROCESSING_DSN, SENTRY_ELASTIC_BEANSTALK_DSN
from constants.common_constants import RUNNING_TEST_OR_FROM_A_SHELL


##### when running in a shell we force sentry off and force the use of the null_error_handler ######
FORCE_NULL_ERROR_HANDLER = False

####################################################################################################

def normalize_sentry_dsn(dsn: str) -> str:
    """ Sentry DSNs may be in a couple weird forms, solve that. """
    if not dsn or "xxxxx" in dsn.lower():  # xxxxx is because sometimes people don't set the DSN
        return ""
    
    # "https://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "sub.domains.sentry.io/yyyyyy"
    prefix, sentry_io = dsn.split("@")
    if sentry_io.count(".") > 1:
        # sub.domains.sentry.io/yyyyyy -> sentry.io/yyyyyy
        sentry_io = ".".join(sentry_io.rsplit(".", 2)[-2:])
    
    return prefix + "@" + sentry_io  # https://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx + @ + sentry.io/yyyyyy


def get_dsn_from_string(sentry_type: str):
    """ Returns a DSN, even if it is incorrectly formatted. """
    try:
        return normalize_sentry_dsn(SentryUtils.TYPE_CHECK[sentry_type])  # type: ignore
    except KeyError:
        raise Exception(f'Invalid sentry type, use {SentryUtils.__module__}.SentryTypes') from None

####################################################################################################


class _SentryUtils:
    """ One stop shop for all sentry usage in the codebase. (Use the instantiation below) """
    
    javascript = "javascript"  # mostly ignore
    
    # These dictate the sentry DSN used for the error sentry, and tags that are automatically added.
    class types:
        data_processing = "data_processing"
        webserver = "webserver"
        forest = "forest"
        push_notifications = "push_notifications"
        script_runner = "script_runner"
    
    # populate this dict with the sentry dsn for each type of error sentry.
    TYPE_CHECK = {
        types.webserver: SENTRY_ELASTIC_BEANSTALK_DSN,
        types.data_processing: SENTRY_DATA_PROCESSING_DSN,
        types.forest: SENTRY_DATA_PROCESSING_DSN,
        types.push_notifications: SENTRY_DATA_PROCESSING_DSN,
        types.script_runner: SENTRY_DATA_PROCESSING_DSN,
    }
    
    # Error Sentry context managers
    # wrap a block with a context manager to send any errors to sentry
    def report_data_processing(self, **tags) -> ErrorSentry:
        return make_error_sentry(self.types.data_processing, **tags)
    def report_webserver(self, **tags) -> ErrorSentry:
        return make_error_sentry(self.types.webserver, **tags)
    def report_forest(self, **tags) -> ErrorSentry:
        return make_error_sentry(self.types.forest, **tags)
    def report_push_notifications(self, **tags) -> ErrorSentry:
        return make_error_sentry(self.types.push_notifications, **tags)
    def report_script_runner(self, **tags) -> ErrorSentry:
        return make_error_sentry(self.types.script_runner, **tags)
    
    # as decorators
    # wrap a function with a decorator to send any errors to sentry
    def decoreport_data_processing(self, **tags) -> Callable:
        return SentryDecorator(self.types.data_processing, **tags)
    def decoreport_webserver(self, **tags) -> Callable:
        return SentryDecorator(self.types.webserver, **tags)
    def decoreport_forest(self, **tags) -> Callable:
        return SentryDecorator(self.types.forest, **tags)
    def decoreport_push_notifications(self, **tags) -> Callable:
        return SentryDecorator(self.types.push_notifications, **tags)
    def decoreport_script_runner(self, **tags) -> Callable:
        return SentryDecorator(self.types.script_runner, **tags)
    
    # Timer Warnings Decorators
    # wrap a function with a timer that sends a warning to sentry if the function takes too long
    def timer_warning_data_processing(self, message: str, seconds: int, **tags) -> SentryTimerWarning:
        return SentryTimerWarning(self.types.data_processing, message, seconds, **tags)
    def timer_warning_webserver(self, message: str, seconds: int, **tags) -> SentryTimerWarning:
        return SentryTimerWarning(self.types.webserver, message, seconds, **tags)
    def timer_warning_forest(self, message: str, seconds: int, **tags) -> SentryTimerWarning:
        return SentryTimerWarning(self.types.forest, message, seconds, **tags)
    def timer_warning_push_notifications(self, message: str, seconds: int, **tags) -> SentryTimerWarning:
        return SentryTimerWarning(self.types.push_notifications, message, seconds, **tags)
    def timer_warning_script_runner(self, message: str, seconds: int, **tags) -> SentryTimerWarning:
        return SentryTimerWarning(self.types.script_runner, message, seconds, **tags)


SentryUtils = _SentryUtils()  # using singleton to reduce some boilerplate


####################################################################################################
###################################### Sentry Error Reporting ######################################
####################################################################################################


# def get_sentry_client(sentry_type: str):
#     return SentryClient(dsn=get_dsn_from_string(sentry_type), transport=HttpTransport)


def make_error_sentry(sentry_type: str, **tags) -> ErrorSentry:
    """ Creates an ErrorSentry, defaults to error limit 10. """
    
    if "tags" in tags and isinstance(tags["tags"], dict):  # fix tags in a named dict
        tags.update(tags.pop("tags"))
    
    if RUNNING_TEST_OR_FROM_A_SHELL or FORCE_NULL_ERROR_HANDLER or "force_null_error_handler" in tags:
        return null_error_handler  # type: ignore[return-value]
    
    tags["code_type"] = sentry_type
    for tagk, tagv in tags.items():  # (how does this work?)
        set_tag(tagk, str(tagv))
    
    # this used to error on invalid DSNs, but now it doesn't and that is a problem because it makes
    # it harder to debug invalid DSNs.
    return ErrorSentry(
        get_dsn_from_string(sentry_type),
        sentry_client_kwargs={'transport': HttpTransport},
        sentry_report_limit=10
    )


def send_sentry_warning(message: str, **tags):
    for tagk, tagv in tags.items():
        set_tag(tagk, str(tagv))
    capture_message(message, level="warning")


####################################################################################################
######################################## Decorators ################################################
####################################################################################################

# not tested (should just work)
def SentryDecorator(sentry_type: str, *sentry_args, **tags):
    """ A decorator that wraps a function with an ErrorSentry. """
    
    def decorator_output_func(func_that_is_wrapped):
        # @functools.wraps(func_that_is_wrapped)  # currently disabled, do stack traces get worse?
        def wrapper_func(*args, **kwargs):
            
            with make_error_sentry(sentry_type, *sentry_args, **tags):
                return func_that_is_wrapped(*args, **kwargs)
        
        return wrapper_func
    return decorator_output_func


class SentryTimerWarning():
    """ Wrap a function with a timer that sends a warning to sentry if the function takes too long.
    (uses a thread and adds up to one-half second of time to the function execution time.) """
    
    def __init__(self, sentry_type: str, message: str, timeout_seconds: int, **tags):
        self.message = message
        self.timeout_seconds = timeout_seconds
        
        if "tags" in tags and isinstance(tags["tags"], dict):  # fix tags in a named dict
            tags.update(tags.pop("tags"))
        tags["code_type"] = sentry_type
        tags["JUST_A_WARNING"] = True
        self.tags = tags
        # todo: work out and document why this is in the __init__? (oorrr here at all?).
        # specifically this one is HttpTransport and the django_settings one is not. I think we need
        # this because it runs during a static context.... but there's no way to get here without
        # importing django settings... maybe because its actually in celery_control...?
        sentry_sdk.init(get_dsn_from_string(sentry_type), transport=HttpTransport)
    
    def __call__(self, some_function):
        self.finished = False
        self.name = some_function.__name__
        
        def wrapper(*args, **kwargs):  # don't @functools.wraps, it made debugging harder.
            thread = Thread(target=self.live_warn, name=f"timer thread for {self.name} {id(self)}")
            thread.start()
            t = timezone.now()
            
            try:
                return some_function(*args, **kwargs)
            finally:
                self.finished = True
                thread.join()  # may be slow
                if (t_total:= (timezone.now() - t).total_seconds()) > self.timeout_seconds:
                    more = f" - call `{self.name}` took {t_total} seconds to run."
                    self.send_warning(more)  # no extra sleep statement, HttpTransport is synchronous
        
        return wrapper
    
    def live_warn(self):
        if RUNNING_TEST_OR_FROM_A_SHELL:
            return
        
        # wait for finished to be set to true, if we go over timeout_seconds send a warning.
        t = timezone.now()
        while not self.finished:
            sleep(0.5)  # we depend on a python `finally` clause
            if (timezone.now() - t).total_seconds() > self.timeout_seconds:
                self.send_warning(f" - call `{self.name}` is currently running over its limit.")
                return
    
    def send_warning(self, more: str):
        for tagk, tagv in self.tags.items():
            set_tag(tagk, str(tagv))
        set_tag("function_name", self.name)
        capture_message(self.message + more, level="warning")
