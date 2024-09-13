from types import FunctionType

from django.http.request import HttpRequest
from django.http.response import HttpResponse


class AbortError(Exception): pass


def abort(http_error_code: int, error_message: str = ""):
    abort_error = AbortError(f"{http_error_code} - '{error_message}'")
    abort_error.error_code = http_error_code
    abort_error.error_message = error_message
    raise abort_error


class AbortMiddleware:
    """ A middleware that mimics the Flask abort() functionality.  Just call abort(http_error_code),
    and, by raising a special error, it stops and sends that response.
    Note: using this middleware triggers the database transaction wrapper to discard all database
    changes made within the body of the view function. """

    def __init__(self, get_response: FunctionType):
        # just following the standard passthrough...
        self.get_response = get_response

    def __call__(self, request: HttpRequest):
        # just following the standard passthrough...
        return self.get_response(request)

    def process_exception(self, request: HttpRequest, exception: Exception):
        # whenever a view raises an exception we check if it is an AbortError, and if so we return
        # and HttpResponse with the appropriate error code
        if isinstance(exception, AbortError):
            # we don't need to return any content, just the status code.
            return HttpResponse(content=b"", status=exception.error_code)
        return None
