""" Original Document sourced from 
https://samuh.medium.com/using-jinja2-with-django-1-8-onwards-9c58fe1204dc """

import re
from datetime import date
from typing import Any, Dict, Optional

from django.contrib.staticfiles.storage import staticfiles_storage
from django.urls import reverse
from jinja2 import Environment
from jinja2.ext import Extension

from config.settings import SENTRY_JAVASCRIPT_DSN
from libs.endpoint_helpers.participant_helpers import niceish_iso_time_format
from libs.utils.dev_utils import p
from libs.utils.http_utils import (astimezone_with_tz, easy_url, fancy_dt_format_with_tz,
    nice_iso_dt_format, time_with_tz)


#
## The entrypoint into Jinja. This gets called by django at application load.
#

def environment(**options: Dict[str, Any]) -> Environment:
    # always, always check for autoescape
    assert "autoescape" in options and options["autoescape"] is True
    
    # trunk-ignore(bandit/B701): no bandit, jinja autoescape is enabled
    env = Environment(
        line_comment_prefix="{#",
        comment_start_string="{% comment %}",
        comment_end_string="{% endcomment %}",
        trim_blocks=True,
        lstrip_blocks=True,
        extensions=[WhiteSpaceCollapser],
        **options
    )
    
    env.globals.update(
        {
            "static": staticfiles_storage.url,
            "url": reverse,
            "easy_url": easy_url,
            "astimezone_with_tz": astimezone_with_tz,
            "time_with_tz": time_with_tz,
            "fancy_dt_format_with_tz": fancy_dt_format_with_tz,
            "nice_iso_dt_format": nice_iso_dt_format,
            "niceish_iso_time_format": niceish_iso_time_format,
            "p": timer,
            "ASSETS": ASSETS,
            "SENTRY_JAVASCRIPT_DSN": SENTRY_JAVASCRIPT_DSN,
            "current_year": date.today().year,
            "len": len,
        }
    )
    return env


## Local and CDN Javascript/CSS libraries.
#  In order to codify the libraries in use we have these two classes.  All templates use these
#  variables to populate any necessary assets loaded onto the page.


class LocalAssets:
    # These assets will be served from the server directly, and should not be minified.
    # Make sure any assets here match the apparent versions in CdnAssets for consistent debugging.
    # (it is very helpful to have real source code available to the IDE.)
    ANGULARJS = "/static/javascript/libraries/angular.js"
    BOOTSTRAP = "/static/javascript/libraries/bootstrap.js"
    BOOTSTRAP_INTEGRITY = "sha256-Cr6N6zNN4bp0OwTQOZ6Z66M2r+2dpy/EwKMCyZ+SOMg="
    BOOTSTRAP_CSS = "/static/css/libraries/bootstrap.css"
    BOOTSTRAP_CSS_INTEGRITY = "7e630d90c7234b0df1729f62b8f9e4bbfaf293d91a5a0ac46df25f2a6759e39a"
    BOOTSTRAP_TIMEPICKER = "/static/javascript/libraries/bootstrap-timepicker.js"
    BOOTSTRAP_TIMEPICKER_CSS = "/static/css/libraries/bootstrap-timepicker.css"
    BOOTSTRAP_DATETIMEPICKER = "/static/javascript/libraries/bootstrap-datetimepicker.js"
    BOOTSTRAP_DATETIMEPICKER_CSS = "/static/css/libraries/bootsetrap-datetimepicker.css"
    DATATABLES = "/static/javascript/libraries/datatables.js"
    DATATABLES_CSS = "/static/css/libraries/datatables.css"
    HANDLEBARS = "/static/javascript/libraries/handlebars.js"
    JQUERY = "/static/javascript/libraries/jquery.js"
    JQUERY_INTEGRITY = "sha256-TQrUBgXESZKk7rT8igyb7U9Y79tnhCTpKa+ryqxXaHc="
    LODASH = "/static/javascript/libraries/lodash.js"
    MOMENTJS = "/static/javascript/libraries/moment.js"


class CdnAssets:
    # These are the assets expected to be used in normal runtime, including most development scenarios.
    # Make sure any assets here match the versions in LocalAssets whenever they are updated
    ANGULARJS = "https://ajax.googleapis.com/ajax/libs/angularjs/1.8.2/angular.min.js"
    BOOTSTRAP = "https://cdn.jsdelivr.net/npm/bootstrap@3.3.7/dist/js/bootstrap.min.js"
    BOOTSTRAP_INTEGRITY = "sha384-Tc5IQib027qvyjSMfHjOMaLkfuWVxZxUPnCJA7l2mCWNIpG9mGCD8wGNIcPD7Txa"
    BOOTSTRAP_CSS = "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css"
    BOOTSTRAP_CSS_INTEGRITY = "sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u"
    BOOTSTRAP_TIMEPICKER = "https://cdn.jsdelivr.net/npm/bootstrap-timepicker@0.5.2/js/bootstrap-timepicker.min.js"
    BOOTSTRAP_TIMEPICKER_CSS = "https://cdn.jsdelivr.net/npm/bootstrap-timepicker@0.5.2/css/bootstrap-timepicker.min.css"
    BOOTSTRAP_DATETIMEPICKER = "https://cdnjs.cloudflare.com/ajax/libs/eonasdan-bootstrap-datetimepicker/4.17.49/js/bootstrap-datetimepicker.min.js"
    BOOTSTRAP_DATETIMEPICKER_CSS = "https://cdnjs.cloudflare.com/ajax/libs/eonasdan-bootstrap-datetimepicker/4.17.49/css/bootstrap-datetimepicker.min.css"
    DATATABLES = "https://cdn.datatables.net/v/dt/dt-1.13.1/cr-1.6.1/r-2.4.0/datatables.min.js"
    DATATABLES_CSS = "https://cdn.datatables.net/v/dt/dt-1.13.1/cr-1.6.1/r-2.4.0/datatables.min.css"
    HANDLEBARS = "https://cdnjs.cloudflare.com/ajax/libs/handlebars.js/4.7.7/handlebars.min.js"
    JQUERY = "https://code.jquery.com/jquery-1.12.4.min.js"
    JQUERY_INTEGRITY = "sha256-ZosEbRLbNQzLpnKIkEdrPv7lOy9C27hHQ+Xp8a4MxAQ="
    LODASH = "https://cdn.jsdelivr.net/npm/lodash@4.17.21/lodash.min.js"
    MOMENTJS = "https://cdnjs.cloudflare.com/ajax/libs/moment.js/2.29.4/moment.min.js"


# set manually to use local assets for debugging purposes only
# ASSETS = LocalAssets
ASSETS = CdnAssets


class WhiteSpaceCollapser(Extension):
    """ Simple Jinja2 extension that collapses whitespace on rendered pages what could possibly go wrong. """
    
    def preprocess(self, source: str, name: Optional[str], filename: Optional[str] = None) -> str:
        # collapse normal horizontal whitespace at the start and end of lines
        return re.sub(r'^[ \t]+|[ \t]+$', '', source, flags=re.MULTILINE)
        
        # collapse sequences of 2+ whitespace characters to a nothing.
        # return re.sub(r'[ \t][ \t]+|\n\n+', '', source, flags=re.MULTILINE)
        
        # collapse most extended whitespace sequences down to just the first character, except
        # newlines, sequences of newlines are collapsed to a single newline. (and register returns)
        # has errors on at least the dashboard page
        # return re.sub(r'[ \t][ \t]+|[\n\r]+[ \t\n\r]*[\n\r]+', '', source, flags=re.MULTILINE)


#
## Hacky but functional debugging/line-profiling tool for templates.
#

def timer(more_label: Any, *args, **kwargs):
    """  The p() profiling function adapted for template rendering.  Usage is different READ.
    - p() is useful because it gives you the line number of the python file.
    - unfortunately it's not that easy in a template.
    - Jinja does some of the work, we can at least identify the file but the line number is wrong.
    - The line number specified is not a static offset, it cannot trivially be accounted for.
    
    extra features:
    - there is a counter that tells you how many p() calls you have run through on this render call.
    
    Usage:
    - stick {{ p(44) }} in your template. using numbers is usually the best strategy.
    
    Output:
    - template name with a wrong line number
    - a counter that increments each time p() is called.
    - the value for a label you passed in.
    - time since previous call in normal p() format
     - frontend/templates/participant.html:317 -- Template - 307 - 45 -- 0.0000134630
    """
    # No. you need that label.
    # if more_label is None:
    #     more_label = "{label recommended, line number is wrong}"
    
    # caller_stack_location=3 results in the name of the template file but with the wrong line number.
    p(
        "rendering",
        *args,
        caller_stack_location=3,
        name=f"Rendering - {COUNTER.the_counter.count} - {more_label}",
        **kwargs
    )
    
    COUNTER.the_counter.increment()
    return ""


class COUNTER:
    """ To improve the time function we need a counter that tracks some Global-ish state. """
    the_counter = None  # global reference
    
    def __init__(self):
        self.count = 1
    
    def increment(self):
        self.count += 1

# initialize the counter
COUNTER.the_counter = COUNTER()
