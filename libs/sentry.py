from __future__ import annotations

# WARNING: THIS FILE IS IMPORTED IN THE DJANGO CONF FILE. BE CAREFUL WITH IMPORTS.
import functools
from threading import Thread
from time import sleep

import sentry_sdk
from cronutils.error_handler import ErrorSentry, null_error_handler
from django.utils import timezone
from sentry_sdk import capture_message, Client as SentryClient, set_tag
from sentry_sdk.transport import HttpTransport

from config.settings import SENTRY_DATA_PROCESSING_DSN, SENTRY_ELASTIC_BEANSTALK_DSN
from constants.common_constants import RUNNING_TEST_OR_FROM_A_SHELL


#### when running in a shell we force sentry off and force the use of the null_error_handler ####


class SentryTypes:
    javascript = "javascript"  # mostly ignore
    
    # These dictate the sentry DSN used for the error sentry, and tags that are automatically added.
    data_processing = "data_processing"
    elastic_beanstalk = "elastic_beanstalk"
    forest = "forest"
    push_notifications = "push_notifications"
    script_runner = "script_runner"
    
    # populate this dict with the sentry dsn for each type of error sentry.
    TYPE_CHECK = {
        elastic_beanstalk: SENTRY_ELASTIC_BEANSTALK_DSN,
        data_processing: SENTRY_DATA_PROCESSING_DSN,
        forest: SENTRY_DATA_PROCESSING_DSN,
        push_notifications: SENTRY_DATA_PROCESSING_DSN,
        script_runner: SENTRY_DATA_PROCESSING_DSN,
    }
    
    # Error Sentry context managers
    # wrap a block with a context manager to send any errors to sentry
    @classmethod
    def error_handler_data_processing(cls, null_error_handler=False, **tags):
        return make_error_sentry(cls.data_processing, null_error_handler, **tags)
    @classmethod
    def error_handler_elastic_beanstalk(cls, null_error_handler=False, **tags):
        return make_error_sentry(cls.elastic_beanstalk, null_error_handler, **tags)
    @classmethod
    def error_handler_forest(cls, null_error_handler=False, **tags):
        return make_error_sentry(cls.forest, null_error_handler, **tags)
    @classmethod
    def error_handler_push_notifications(cls, null_error_handler=False, **tags):
        return make_error_sentry(cls.push_notifications, null_error_handler, **tags)
    @classmethod
    def error_handler_script_runner(cls, null_error_handler=False, **tags):
        return make_error_sentry(cls.script_runner, null_error_handler, **tags)
    
    # as decorators
    # wrap a function with a decorator to send any errors to sentry
    @classmethod
    def error_decor_data_processing(cls, null_error_handler=False, **tags):
        return SentryDecorator(cls.data_processing, null_error_handler, **tags)
    @classmethod
    def error_decor_elastic_beanstalk(cls, null_error_handler=False, **tags):
        return SentryDecorator(cls.elastic_beanstalk, null_error_handler, **tags)
    @classmethod
    def error_decor_forest(cls, null_error_handler=False, **tags):
        return SentryDecorator(cls.forest, null_error_handler, **tags)
    @classmethod
    def error_decor_push_notifications(cls, null_error_handler=False, **tags):
        return SentryDecorator(cls.push_notifications, null_error_handler, **tags)
    @classmethod
    def error_decor_script_runner(cls, null_error_handler=False, **tags):
        return SentryDecorator(cls.script_runner, null_error_handler, **tags)
    
    # Timer Warnings Decorators
    # wrap a function with a timer that sends a warning to sentry if the function takes too long
    @classmethod
    def timer_warning_data_processing(cls, message: str, seconds: int, **tags):
        return SentryTimerWarning(cls.data_processing, message, seconds, **tags)
    @classmethod
    def timer_warning_elastic_beanstalk(cls, message: str, seconds: int, **tags):
        return SentryTimerWarning(cls.elastic_beanstalk, message, seconds, **tags)
    @classmethod
    def timer_warning_forest(cls, message: str, seconds: int, **tags):
        return SentryTimerWarning(cls.forest, message, seconds, **tags)
    @classmethod
    def timer_warning_push_notifications(cls, message: str, seconds: int, **tags):
        return SentryTimerWarning(cls.push_notifications, message, seconds, **tags)
    @classmethod
    def timer_warning_script_runner(cls, message: str, seconds: int, **tags):
        return SentryTimerWarning(cls.script_runner, message, seconds, **tags)


def normalize_sentry_dsn(dsn: str):
    if not dsn:
        return dsn
    # "https://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "sub.domains.sentry.io/yyyyyy"
    prefix, sentry_io = dsn.split("@")
    if sentry_io.count(".") > 1:
        # sub.domains.sentry.io/yyyyyy -> sentry.io/yyyyyy
        sentry_io = ".".join(sentry_io.rsplit(".", 2)[-2:])
    # https://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx + @ + sentry.io/yyyyyy"
    return prefix + "@" + sentry_io


def get_dsn_from_string(sentry_type: str):
    """ Returns a DSN, even if it is incorrectly formatted. """
    try:
        return normalize_sentry_dsn(SentryTypes.TYPE_CHECK[sentry_type])  # type: ignore
    except KeyError:
        raise Exception(f'Invalid sentry type, use {SentryTypes.__module__}.SentryTypes') from None


def get_sentry_client(sentry_type: str):
    return SentryClient(dsn=get_dsn_from_string(sentry_type), transport=HttpTransport)


def make_error_sentry(sentry_type: str, force_null_error_handler=False, **tags) -> ErrorSentry:
    """ Creates an ErrorSentry, defaults to error limit 10. """
    
    if RUNNING_TEST_OR_FROM_A_SHELL or force_null_error_handler:
        return null_error_handler  # type: ignore[return-value]
    
    if "tags" in tags and isinstance(tags["tags"], dict):  # fix tags in a named dict
        tags.update(tags.pop("tags"))
    
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
    (uses a thread and adds up to one-half second of time to the function execution time.)
    """
    
    def __init__(self, sentry_type: str, message: str, timeout_seconds: int, **tags):
        self.message = message
        self.timeout_seconds = timeout_seconds
        
        if "tags" in tags and isinstance(tags["tags"], dict):  # fix tags in a named dict
            tags.update(tags.pop("tags"))
        tags["code_type"] = sentry_type
        tags["JUST_A_WARNING"] = True
        self.tags = tags
        
        # todo: work out and document why this is in the __init__?
        sentry_sdk.init(get_dsn_from_string(sentry_type), transport=HttpTransport)
    
    def __call__(self, some_function):
        self.finished = False
        self.name = some_function.__name__
        
        # @functools.wraps(some_function)  # currently disabled, do stack traces get worse?
        def wrapper(*args, **kwargs):
            
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
                    self.send_warning(more)
                    #TODO: um, do we need a sleep here? that gets weird with post-celery-task-sleep
        
        return wrapper
    
    def live_warn(self):
        if RUNNING_TEST_OR_FROM_A_SHELL:
            return
        
        # wait for finished to be set to true, if we go over timeout_seconds send a warning.
        t = timezone.now()
        while not self.finished:
            sleep(0.5)  # can't get out of a dangling join above, hope try-finally is robust
            if (timezone.now() - t).total_seconds() > self.timeout_seconds:
                self.send_warning(f" - call `{self.name}` is currently running over its limit.")
                return
    
    def send_warning(self, more: str):
        for tagk, tagv in self.tags.items():
            set_tag(tagk, str(tagv))
        set_tag("function_name", self.name)
        capture_message(self.message + more, level="warning")
