# WARNING: THIS FILE IS IMPORTED IN THE DJANGO CONF FILE. BE CAREFUL WITH IMPORTS.
import functools
from datetime import datetime

import sentry_sdk
from cronutils.error_handler import ErrorSentry, null_error_handler
from sentry_sdk import capture_message, Client as SentryClient, set_tag
from sentry_sdk.transport import HttpTransport

from config.settings import (SENTRY_DATA_PROCESSING_DSN, SENTRY_ELASTIC_BEANSTALK_DSN,
    SENTRY_JAVASCRIPT_DSN)
from constants.common_constants import RUNNING_TEST_OR_FROM_A_SHELL


# when running in a shell we force sentry off and force the use of the null_error_handler


class SentryTypes:
    # if you have to go update get_dsn_from_string() if you update this.
    data_processing = "data_processing"
    elastic_beanstalk = "elastic_beanstalk"
    javascript = "javascript"
    script_runner = "script_runner"


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
    if sentry_type in (SentryTypes.data_processing, SentryTypes.script_runner):
        return normalize_sentry_dsn(SENTRY_DATA_PROCESSING_DSN)
    elif sentry_type == SentryTypes.elastic_beanstalk:
        return normalize_sentry_dsn(SENTRY_ELASTIC_BEANSTALK_DSN)
    elif sentry_type == SentryTypes.javascript:
        return normalize_sentry_dsn(SENTRY_JAVASCRIPT_DSN)
    else:
        raise Exception(f'Invalid sentry type, use {SentryTypes.__module__}.SentryTypes')


def get_sentry_client(sentry_type: str):
    dsn = get_dsn_from_string(sentry_type)
    return SentryClient(dsn=dsn, transport=HttpTransport)


def make_error_sentry(sentry_type: str, tags: dict = None, force_null_error_handler=False):
    """ Creates an ErrorSentry, defaults to error limit 10.
    If the applicable sentry DSN is missing will return an ErrorSentry,
    but if null truthy a NullErrorHandler will be returned instead. """
    
    if RUNNING_TEST_OR_FROM_A_SHELL or force_null_error_handler:
        return null_error_handler
    
    tags = tags or {}
    tags["sentry_type"] = sentry_type
    for tagk, tagv in tags.items():  # (how does this work?)
        set_tag(tagk, str(tagv))
    
    # this used to error on invalid DSNs, but now it doesn't and that is a problem because it makes
    # it harder to debug invalid DSNs.
    return ErrorSentry(
        get_dsn_from_string(sentry_type),
        sentry_client_kwargs={'transport': HttpTransport},
        sentry_report_limit=10
    )


def elastic_beanstalk_error_sentry(*args, **kwargs) -> ErrorSentry:
    return make_error_sentry(SentryTypes.elastic_beanstalk, *args, **kwargs)


def data_processing_error_sentry(*args, **kwargs) -> ErrorSentry:
    return make_error_sentry(SentryTypes.data_processing, *args, **kwargs)


def script_runner_error_sentry(*args, **kwargs) -> ErrorSentry:
    return make_error_sentry(SentryTypes.script_runner, *args, **kwargs)


####################################################################################################


class send_warn_on_timer():
    """ Wrap a function with a timer that sends a warning to sentry if the function takes too long. """
    
    def __init__(self, sentry_type: str, message: str, timeout_seconds: int,  tags: dict = None):
        self.message = message
        self.timeout_seconds = timeout_seconds
        self.tags = tags or {}
        self.tags["sentry_type"] = sentry_type
        self.tags["JUST_A_WARNING"] = True
        sentry_sdk.init(get_dsn_from_string(sentry_type), transport=HttpTransport)
    
    def __call__(self, some_function):
        
        @functools.wraps(some_function)
        def wrapper(*args, **kwargs):
            t = datetime.now()
            try:
                return some_function(*args, **kwargs)
            finally:
                if (t_total:= (datetime.now() - t).total_seconds()) > self.timeout_seconds:
                    for tagk, tagv in self.tags.items():
                        set_tag(tagk, str(tagv))
                    set_tag("function_name", some_function.__name__)
                    
                    ext = f" - call `{some_function.__name__}` took {t_total} seconds to run."
                    capture_message(self.message + ext, level="warning")
        
        return wrapper


def time_warning_elastic_beanstalk(message: str, timeout_seconds: int,  tags: dict = None) -> send_warn_on_timer:
    return send_warn_on_timer(SentryTypes.elastic_beanstalk, message, timeout_seconds, tags)


def time_warning_data_processing(message: str, timeout_seconds: int,  tags: dict = None) -> send_warn_on_timer:
    return send_warn_on_timer(SentryTypes.data_processing, message, timeout_seconds, tags)


def time_warning_script_runner(message: str, timeout_seconds: int,  tags: dict = None) -> send_warn_on_timer:
    return send_warn_on_timer(SentryTypes.script_runner, message, timeout_seconds, tags)
