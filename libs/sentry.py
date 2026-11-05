from __future__ import annotations

# WARNING: THIS FILE IS IMPORTED IN THE DJANGO CONF FILE. BE CAREFUL WITH IMPORTS.
import atexit
from collections.abc import Callable
from threading import Lock, Thread, local
from time import sleep

import sentry_sdk
from cronutils.error_handler import ErrorHandler, null_error_handler
from django.utils import timezone
from sentry_sdk.client import BaseClient
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.django import DjangoIntegration
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
    def report_data_processing(self, **tags) -> SentryErrorHandler:
        return make_error_sentry(self.types.data_processing, **tags)
    def report_webserver(self, **tags) -> SentryErrorHandler:
        return make_error_sentry(self.types.webserver, **tags)
    def report_forest(self, **tags) -> SentryErrorHandler:
        return make_error_sentry(self.types.forest, **tags)
    def report_push_notifications(self, **tags) -> SentryErrorHandler:
        return make_error_sentry(self.types.push_notifications, **tags)
    def report_script_runner(self, **tags) -> SentryErrorHandler:
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
########################################## Sentry Clients ##########################################
####################################################################################################

# Sentry has one process-wide ("global") client. Integrations (Django, Celery, uncaught exceptions)
# and any sentry call made outside of a SentryUtils.report_* block report to it. It is set once per
# process by init_global_sentry: the webserver in django_settings, celery workers in their modules.
#
# SentryUtils.report_* blocks and timer warnings report to the client for their sentry type by
# activating it in a new scope. They must never call sentry_sdk.init: that replaces the global client
# with a new one, discarding the options below and routing all later errors in the process to
# whichever DSN was used most recently.

_CACHED_CLIENTS: dict[str, BaseClient] = {}
_CACHED_CLIENTS_LOCK = Lock()


def sentry_client_options() -> dict:
    """ Options for every sentry client in the codebase. (Integrations are instantiated per client.) """
    return {
        "enable_tracing": False,
        "ignore_errors": ["WorkerLostError", "DisallowedHost"],
        "transport": HttpTransport,
        # TODO: get error filtering working
        # "before_send": filter_junk_errors,
        "integrations": [
            DjangoIntegration(
                transaction_style='url',
                middleware_spans=False,
                signals_spans=False,
                cache_spans=False,
            ),
            CeleryIntegration(
                propagate_traces=False,
                monitor_beat_tasks=False,
                exclude_beat_tasks=True,  # type: ignore - this value seems to work fine
            ),
        ],
    }


def init_global_sentry(sentry_type: str):
    """ Sets the process-wide sentry client. Call once, at process startup. """
    sentry_sdk.init(dsn=get_dsn_from_string(sentry_type), **sentry_client_options())


def init_worker_global_sentry(sentry_type: str):
    """ For celery worker modules, which are also imported by tests and shells. """
    if not RUNNING_TEST_OR_FROM_A_SHELL:
        init_global_sentry(sentry_type)


def get_sentry_client(sentry_type: str) -> BaseClient | None:
    """ Returns the client for a sentry type, reusing the global client if it has the same DSN.
    Returns None if the DSN is not configured, the caller should then use the current client. """
    dsn = get_dsn_from_string(sentry_type)
    if not dsn:
        return None

    global_client = sentry_sdk.get_global_scope().client
    if global_client.is_active() and global_client.options["dsn"] == dsn:
        return global_client

    with _CACHED_CLIENTS_LOCK:
        if dsn not in _CACHED_CLIENTS:
            if not _CACHED_CLIENTS:
                atexit.register(_flush_cached_clients)
            _CACHED_CLIENTS[dsn] = sentry_sdk.Client(dsn, **sentry_client_options())
        return _CACHED_CLIENTS[dsn]


def _flush_cached_clients():
    """ Sentry's own atexit hook only flushes the global client. """
    for client in _CACHED_CLIENTS.values():
        client.flush()


####################################################################################################
###################################### Sentry Error Reporting ######################################
####################################################################################################


# This Sentry client caching was implemented by Claude Opus 5.5, I'm running it to identify whether
# it improves the error reporting details intended in the prior setup where it would automatically
# tag my source server (frontend, data processing, etc.), should fix the cases where running in a
# shell reported errors, all of which probably got reset so only worked periodically.

# Licencing probably doesn't matter, here, but if it does the intent is that this derivative work
# follows the licensing terms of the zagaran/cronutils project.
# See the repo here: https://github.com/zagaran/cronutils/blob/master/LICENSE

class SentryErrorHandler(ErrorHandler):
    """ A cronutils ErrorHandler that reports errors to the sentry client of a sentry type.
    Replaces cronutils' ErrorSentry, which calls sentry_sdk.init on creation (see Sentry Clients).
    Each `with` block runs in a new sentry scope with the type's client and the tags, so the tags
    only apply to events from inside the block. Instances can be reused, including across threads. """

    def __init__(self, sentry_type: str, sentry_report_limit: int = 10, **tags):
        super().__init__()
        self.sentry_type = sentry_type
        self.sentry_report_limit = sentry_report_limit  # per-stacktrace, less than 1 means no limit
        self.tags = tags
        self._thread_state = local()  # stack of open scopes, per thread; !! not same as `locals()`

    def __enter__(self):
        if not hasattr(self._thread_state, "scope_managers"):
            self._thread_state.scope_managers = []

        scope_manager = sentry_sdk.new_scope()
        scope = scope_manager.__enter__()
        self._thread_state.scope_managers.append(scope_manager)

        if (client := get_sentry_client(self.sentry_type)) is not None:
            scope.set_client(client)
        for tag_key, tag_value in self.tags.items():
            scope.set_tag(tag_key, str(tag_value))
        return self

    def __exit__(self, exec_type, exec_value, traceback) -> bool:
        try:
            ret = super().__exit__(exec_type, exec_value, traceback)
            if ret and isinstance(exec_value, Exception):
                # (the stacktrace key is guaranteed to exist after the super call)
                error_count = len(self.errors[self.format_traceback_key(exec_value, traceback)])
                if self.sentry_report_limit < 1 or error_count <= self.sentry_report_limit:
                    sentry_sdk.capture_exception(exec_value)
            return ret
        finally:
            self._thread_state.scope_managers.pop().__exit__(None, None, None)


def make_error_sentry(sentry_type: str, **tags) -> SentryErrorHandler:
    """ Creates a SentryErrorHandler, defaults to error limit 10. """

    if "tags" in tags and isinstance(tags["tags"], dict):  # fix tags in a named dict
        tags.update(tags.pop("tags"))

    if RUNNING_TEST_OR_FROM_A_SHELL or FORCE_NULL_ERROR_HANDLER or "force_null_error_handler" in tags:
        return null_error_handler  # type: ignore[return-value]

    tags["code_type"] = sentry_type
    return SentryErrorHandler(sentry_type, **tags)


def send_sentry_warning(message: str, **tags):
    """ Reports to the current client: the enclosing report_* block's client, else the global one. """
    with sentry_sdk.new_scope() as scope:
        for tagk, tagv in tags.items():
            scope.set_tag(tagk, str(tagv))
        sentry_sdk.capture_message(message, level="warning")


####################################################################################################
######################################## Decorators ################################################
####################################################################################################

# not tested (should just work)
def SentryDecorator(sentry_type: str, *sentry_args, **tags):
    """ A decorator that wraps a function with a SentryErrorHandler. """

    def decorator_output_func(func_that_is_wrapped):
        # @functools.wraps(func_that_is_wrapped)  # currently disabled, do stack traces get worse?
        def wrapper_func(*args, **kwargs):

            with make_error_sentry(sentry_type, *sentry_args, **tags):
                return func_that_is_wrapped(*args, **kwargs)

        return wrapper_func
    return decorator_output_func


class SentryTimerWarning:
    """ Wrap a function with a timer that sends a warning to sentry if the function takes too long.
    (uses a thread and adds up to one-half second of time to the function execution time.) """

    def __init__(self, sentry_type: str, message: str, timeout_seconds: int, **tags):
        self.sentry_type = sentry_type
        self.message = message
        self.timeout_seconds = timeout_seconds

        if "tags" in tags and isinstance(tags["tags"], dict):  # fix tags in a named dict
            tags.update(tags.pop("tags"))
        tags["code_type"] = sentry_type
        tags["JUST_A_WARNING"] = True
        self.tags = tags

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
        with sentry_sdk.new_scope() as scope:
            if (client := get_sentry_client(self.sentry_type)) is not None:
                scope.set_client(client)
            for tagk, tagv in self.tags.items():
                scope.set_tag(tagk, str(tagv))
            scope.set_tag("function_name", self.name)
            sentry_sdk.capture_message(self.message + more, level="warning")
