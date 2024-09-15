# WARNING: THIS FILE IS IMPORTED IN THE DJANGO CONF FILE. BE CAREFUL WITH IMPORTS.

from cronutils.error_handler import ErrorSentry, null_error_handler
from sentry_sdk import Client as SentryClient, set_tag
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
    
    # set tags? we don't know if this works
    for tagk, tagv in tags.items():
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
