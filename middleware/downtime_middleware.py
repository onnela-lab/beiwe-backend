from types import FunctionType

from django.http.request import HttpRequest
from django.http.response import HttpResponse

from database.system_models import GlobalSettings


class DowntimeMiddleware:
    """ A very quick and dirty downtime middleware.  If downtime is enabled, it returns a 503. """
    
    def __init__(self, get_response: FunctionType):
        # just following the standard passthrough...
        self.get_response = get_response
    
    def __call__(self, request: HttpRequest):
        # if downtime is enabled, return a 503.
        if GlobalSettings.singleton().downtime_enabled:
            return HttpResponse(
                content="This server is currently undergoing maintenance, please try again later.",
                status=503
            )
        return self.get_response(request)
