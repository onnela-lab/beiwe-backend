import json
from types import FunctionType

from django.http.request import HttpRequest


class CurlMiddleware:
    """ A midleware that mimics the Flask abort() functionality.  Just call abort(http_error_code),
    and, by raising a special error, it stops and sends that response. """
    
    def __init__(self, get_response: FunctionType):
        # just following the standard passthrough...
        self.get_response = get_response
    
    def __call__(self, request: HttpRequest):
        try:
            curl_output = get_curl(request)
            print("incoming request as curl command:")
            print(curl_output)
            print()
        except AssertionError as e:
            print("could not format incoming request as curl command:")
            print(e)
            print()
        return self.get_response(request)


"""
Convert a Django HTTPRequest object (or dictionary of such a request?) into a cURL command.
Based on the code at https://gist.github.com/asfaltboy/8df5cc73c63d897ba6344e41ee2e10b5
It is assumed no license or Public Domain, this is a debugging tool.
"""

REQUIRED_FIELDS = ['META', 'META.REQUEST_METHOD', 'META.SERVER_NAME', 'META.PATH_INFO']


def convert_header_names(word: str, delim='-') -> str:
    """ header name formatting conversion """
    return delim.join(x.capitalize() or '_' for x in word.split('_'))


def get_request_dict(request: str | bytes | HttpRequest | dict) -> dict[str, str]:
    """ returns a dictionary reqresentation of the request object from several source formats. """
    if isinstance(request, (str, bytes)):
        try:
            return json.loads(request)
        except Exception:
            print('Must be given a valid JSON')
            raise
    if not isinstance(request, dict):
        return vars(request)
    return request


def get_headers(request: dict[str, str]) -> tuple[str, dict[str, str]]:
    """ Extracts http headers from a request dictionary, escapes double quotes. """
    host = None
    headers = {}
    for name, value in request['META'].items():
        if name == "HTTP_HOST":
            host = value
            continue  # comment to preserve host header, but eventual output contains host twice.
        if name.startswith('HTTP_'):
            headers[convert_header_names(name[5:])] = value.replace('"', r'\"')
    assert host is not None, "HTTP_HOST not found in request headers."
    return host, headers


def validate_request(
    request: HttpRequest, required_attributes: list[str] = REQUIRED_FIELDS, prefix=''
):
    for field in required_attributes:
        if '.' in field:
            parts = field.split('.')
            validate_request(
                request[parts[0]],
                required_attributes=['.'.join(parts[1:])],
                prefix=f'{parts[0]}.'
            )
            continue
        assert field in request, f'The `request.{prefix}{field}` attribute is required'


def get_curl(request: HttpRequest):
    """ Returns the full text of a curl command to replicate a request. """
    # extract data, run safety checks
    request = get_request_dict(request)
    meta = request['META']
    validate_request(request)
    assert meta['REQUEST_METHOD'] == 'GET', 'Only GET currently supported'
    
    # all components of the command
    url = f"{meta['SERVER_NAME']}{meta['PATH_INFO']}?{meta['QUERY_STRING']}"
    host, headers = get_headers(request)
    headers = ' '.join(f'  -H "{h}: {v}" \\\n' for h, v in headers.items())
    
    # there's some spam that you get when curl is targeting non-default ports, this line is at the
    # end of the copied section and the user can opt whether to include this.  (I have been unable
    # to otherwise suppress this message.)
    dev_null = "\\\n  2> /dev/null  # ports other than 80 prints a spam error."
    
    return f'curl {host} {headers} "{url}" {dev_null}'
