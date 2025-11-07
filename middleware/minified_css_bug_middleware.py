from types import FunctionType
import logging
from django.http.request import HttpRequest
from django.http.response import HttpResponse
from jinja2.exceptions import TemplateNotFound


class MissingMinifiedCSSError(Exception): pass

css_bug_log = logging.getLogger("celery_runtime")
css_bug_log.setLevel(logging.ERROR)

ERROR_MESSAGE = b"""
<style>html, body{background-color: #121212; color: #ffffff;} </style>

Server Error

<br><br>

Missing minified CSS assets

<br><br>

The most likely cause of this error is a known bug where a manual action taken by a system
administrator deployed correctly, but operations that should occur to generate assets required by
the webserver at that time were not executed by the server.

<br><br>

The fix this issue, redeploy the server using the standard deployment command, or, if running via an
AWS Elastic Beanstalk environment, the administrator can run a Deploy operation of the most recent
Application version in the `Application versions` section on the AWS Online Console for your
environment.

<br><br>

If this does not resolve the issue please file a bug report on the Beiwe Backend Github repository.

<br><br>

Apologies for the inconvenience - The Beiwe Developers
"""


class MinifiedCSSMiddleware:
    
    def __init__(self, get_response: FunctionType):
        # just following the standard passthrough...
        self.get_response = get_response
    
    def __call__(self, request: HttpRequest):
        # just following the standard passthrough...
        return self.get_response(request)
    
    def process_exception(self, request: HttpRequest, exception: Exception):
        if isinstance(exception, TemplateNotFound):
            if ".min.css" in str(exception):
                css_bug_log.error(f"MinifiedCSSMiddleware caught exception: {exception}")
                return HttpResponse(content=ERROR_MESSAGE, status=500)
                # raise Exception("Minified CSS file not found.") from None
        
        return None