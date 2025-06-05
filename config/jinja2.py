""" Original Document sourced from 
https://samuh.medium.com/using-jinja2-with-django-1-8-onwards-9c58fe1204dc """

import re
from datetime import date
from typing import Any

from django.conf import settings
from django.contrib.staticfiles.storage import staticfiles_storage
from django.urls import reverse
from jinja2 import Environment
from jinja2.ext import Extension

from config.settings import SENTRY_JAVASCRIPT_DSN
from database.common_models import numformat
from libs.endpoint_helpers.participant_helpers import niceish_iso_time_format
from libs.utils.dev_utils import p
from libs.utils.http_utils import (astimezone_with_tz, easy_url, fancy_dt_format_with_tz,
    nice_iso_dt_format, time_with_tz)


#
## The entrypoint into Jinja. This gets called by django at application load.
#

def environment(**options: dict[str, Any]) -> Environment:
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
            "ASSETS": CdnAssets,
            "LocalAssets": LocalAssets,
            "SENTRY_JAVASCRIPT_DSN": SENTRY_JAVASCRIPT_DSN,
            "current_year": date.today().year,
            "len": len,
            "min": min,
            "max": max,
            "abs": abs,
        }
    )
    return env


## Local and CDN Javascript/CSS libraries.
#  In order to codify the libraries in use we have these two classes.  All templates use these
#  variables to populate any necessary assets loaded onto the page.


class LocalAssets:
    # These assets will be served from the server directly.
    # Make sure any assets here match the apparent versions
    BOOTSTRAP_TIMEPICKER_CSS = "css/libraries/bootstrap-timepicker.css"
    BOOTSTRAP_DATETIMEPICKER_CSS = "css/libraries/bootstrap-datetimepicker.css"
    DATATABLES_CSS = "css/libraries/datatables.css"
    DARKLY = "css/libraries/bootstrap-darkly.css"
    ADMIN = "css/admin.css"
    DASHBOARD_CSS = "css/dashboard_pages.css"
    
    # JS
    ANGULAR_APP_JS = "javascript/app/app.module.js"
    TRANSITION_JS = "javascript/libraries/transition.js"
    COLLAPSE_JS = "javascript/libraries/collapse.js"
    DATA_ACCESS_WEB_FORM_CONTROLLER = "javascript/app/survey-builder/controllers/data-access-web-form-controller.js"
    DATA_DOWNLOAD_PAGE = "javascript/data_download_page.js"
    PARTICIPANTS_TABLE_JS = "javascript/participants_table.js"
    PATIENT_DASHBOARD_FEATURES_JS = "patient_dashboard_features.js"
    NAVBAR_JS = "javascript/app/survey-builder/controllers/nav-bar-study-controller.js"


# until we are reading to use minified assets on production this is debug-only
# if settings.DEBUG:
#     for attrname in dir(LocalAssets):
#         if attrname.startswith("_"):
#             continue
#         attr = getattr(LocalAssets, attrname)
#         if isinstance(attr, str):
#             if attr.endswith(".css"):
#                 setattr(LocalAssets, attrname, attr.replace(".css", ".min.css"))
#             # elif attr.endswith(".js"):
#             #     setattr(LocalAssets, attrname, attr.replace(".js", ".min.js"))


class CdnAssets:
    # These are the assets expected to be used in normal runtime, including most development scenarios.
    # Make sure any assets here match the versions in LocalAssets whenever they are updated
    ANGULARJS = "https://ajax.googleapis.com/ajax/libs/angularjs/1.8.2/angular.min.js"
    BOOTSTRAP = "https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/3.4.1/js/bootstrap.min.js"
    BOOTSTRAP_INTEGRITY = "sha512-oBTprMeNEKCnqfuqKd6sbvFzmFQtlXS3e0C/RGFV0hD6QzhHV+ODfaQbAlmY6/q0ubbwlAM/nCJjkrgA3waLzg=="
    BOOTSTRAP_TIMEPICKER = "https://cdn.jsdelivr.net/npm/bootstrap-timepicker@0.5.2/js/bootstrap-timepicker.min.js"
    BOOTSTRAP_DATETIMEPICKER = "https://cdnjs.cloudflare.com/ajax/libs/eonasdan-bootstrap-datetimepicker/4.17.49/js/bootstrap-datetimepicker.min.js"
    BOOTSTRAP_DATETIMEPICKER_CSS = "https://cdnjs.cloudflare.com/ajax/libs/eonasdan-bootstrap-datetimepicker/4.17.49/css/bootstrap-datetimepicker.min.css"
    DATATABLES = "https://cdn.datatables.net/v/dt/dt-1.13.1/cr-1.6.1/r-2.4.0/datatables.min.js"
    HANDLEBARS = "https://cdnjs.cloudflare.com/ajax/libs/handlebars.js/4.7.7/handlebars.min.js"
    JQUERY = "https://code.jquery.com/jquery-1.12.4.min.js"
    JQUERY_INTEGRITY = "sha256-ZosEbRLbNQzLpnKIkEdrPv7lOy9C27hHQ+Xp8a4MxAQ="
    LODASH = "https://cdn.jsdelivr.net/npm/lodash@4.17.21/lodash.min.js"
    MOMENTJS = "https://cdnjs.cloudflare.com/ajax/libs/moment.js/2.29.4/moment.min.js"


class WhiteSpaceCollapser(Extension):
    """ Simple Jinja2 extension that collapses whitespace on rendered pages what could possibly go wrong. """
    
    def preprocess(self, source: str, name: str | None, filename: str = None) -> str:
        
        # do not collapse whitespace in minified javascript files
        if name and name.endswith(".min.css") or name.endswith(".min.js"):
            return source
        
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
