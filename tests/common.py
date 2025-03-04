import logging
import os
import traceback
from datetime import datetime
from functools import wraps
from itertools import chain
from os.path import join as path_join
from pprint import pprint
from sys import argv
from typing import Callable, Dict

from deepdiff import DeepDiff
from django.contrib import messages
from django.core.exceptions import ImproperlyConfigured
from django.db.models import Model
from django.http.response import HttpResponse, HttpResponseRedirect
from django.test import TestCase
from django.urls import reverse
from django.urls.base import resolve
from django.urls.exceptions import NoReverseMatch

from authentication.tableau_authentication import X_ACCESS_KEY_ID, X_ACCESS_KEY_SECRET
from constants.testing_constants import REAL_ROLES, ResearcherRole
from database.security_models import ApiKey
from database.study_models import Study
from database.user_models_participant import Participant, SurveyNotificationReport
from database.user_models_researcher import Researcher, StudyRelation
from libs.internal_types import ResponseOrRedirect, StrOrBytes
from libs.shell_support import diff_strings, tformat
from libs.utils.security_utils import generate_easy_alphanumeric_string
from tests.helpers import compare_dictionaries, DatabaseHelperMixin, render_test_html_file
from urls import urlpatterns


# trunk-ignore-all(ruff/B018)

# if we import this from constants.url_constants then its not populated because ... Django.
ENDPOINTS_BY_NAME = {pattern.name: pattern for pattern in urlpatterns}

# this makes print statements during debugging easier to read by bracketing the statement of which
# test is running with some separator.
VERBOSE_2_OR_3 = ("-v2" in argv or "-v3" in argv) and "-v1" not in argv
VERBOSE_3 = "-v3" in argv and "-v2" not in argv and "-v1" not in argv


# trunk-ignore(ruff/E402,ruff/E703)
from libs import s3;  # (the ; and this comment blocks automatic reformatting of imports here.
s3.S3_BUCKET = Exception   # force disable potentially active s3 connections.


# 2023-11-21: for unknown reasons importing oak, jasmine, or willow from Forest anywhere at all
# (currently that is limited to the celery forest, so this doesn't happen on webserver code) causes
# the django requests logger to be set to WARNING, which causes a lot of noise in the test output.
# This is not ideal, forest shouldn't do that, but we don't know why this is happening so for now
# we'll just force it to logging.ERROR. (Investigating this revealed some super weird behavior of
# the python logging library.)
# see https://github.com/onnela-lab/forest/issues/217
# forest commit at the time: 810ef6c1f2779c46be402819fd807402b6769387
logging.getLogger("django.request").setLevel(logging.ERROR)


class MisconfiguredTestException(Exception):
    pass


# This parameter sets the password iteration count, which directly adds to the runtime of ALL user
# tests. If we use the default value it is 1000s of times slower and tests take forever.
Researcher.DESIRED_ITERATIONS = 2
Participant.DESIRED_ITERATIONS = 2
ApiKey.DESIRED_ITERATIONS = 2


class CommonTestCase(TestCase, DatabaseHelperMixin):
    """ This class contains the various test-oriented features, for example the assert_present
    method that handles a common case of some otherwise distracting type coersion. """
    
    def monkeypatch_messages(self, function: callable):
        """ This function wraps the messages library and directs it to the terminal for easy
        behavior identification, the original function is then called. """
        
        def intercepted(request, message, extra_tags='', fail_silently=False):
            if VERBOSE_2_OR_3:
                print(f"from messages.{function.__name__}(): '{message}'")
            self.messages.append(message)
            return function(request, message, extra_tags=extra_tags, fail_silently=fail_silently)
        
        return intercepted
    
    def setUp(self) -> None:
        # Patch messages to print to stash any message text for later inspection. (extremely fast)
        self.messages = []
        messages.debug = self.monkeypatch_messages(messages.debug)
        messages.info = self.monkeypatch_messages(messages.info)
        messages.success = self.monkeypatch_messages(messages.success)
        messages.warning = self.monkeypatch_messages(messages.warning)
        messages.error = self.monkeypatch_messages(messages.error)
        
        if VERBOSE_2_OR_3:
            print("\n==")
        return super().setUp()
    
    def tearDown(self) -> None:
        if VERBOSE_2_OR_3:
            print("==")
        return super().tearDown()
    
    def assert_message(self, expected_message: str):
        """ Convenience assertion for whether messages was called with a value. """
        self.assertIn(expected_message, self.messages)
    
    def assert_message_fragment(self, message_fragment):
        """ Convenience assertion for whether messages was called with a value, but checks whether
        any message contains a substring. """
        for message in self.messages:
            if message_fragment in message:
                return
        assert False, f"message fragment '{message_fragment}' not found in any messages."
    
    @property
    def clear_messages(self):
        self.messages = []
    
    def assert_response_url_equal(self, a: str, b: str):
        # when a url comes in from a response object (e.g. response.url) the / characters are
        # encoded in html escape format.  This causes an error in the call to resolve
        a = a.replace(r"%2F", "/")
        b = b.replace(r"%2F", "/")
        resolve_a, resolve_b, = resolve(a), resolve(b)
        msg = f"urls do not point to the same function:\n a - {a}, {resolve_a}\nb - {b}, {resolve_b}"
        return self.assertIs(resolve(a).func, resolve(b).func, msg)
    
    def assert_not_present(self, test_str: StrOrBytes, corpus: StrOrBytes):
        """ Tests "in" and also handles the type coersion for bytes and strings, and suppresses 
        excessively long output that can occur when testing for presence of substrings in html."""
        return self._assert_present(False, test_str, corpus)
    
    def assert_present(self, test_str: StrOrBytes, corpus: StrOrBytes):
        """ Tests "not in" and also handles the type coersion for bytes and strings, and suppresses 
        excessively long output that can occur when testing for presence of substrings in html."""
        return self._assert_present(True, test_str, corpus)
    
    def _assert_present(self, the_test: bool, test_str: StrOrBytes, corpus: StrOrBytes):
        t_test = type(test_str)
        t_corpus = type(corpus)
        test_str = test_str.encode() if t_test == str and t_corpus == bytes else test_str
        test_str = test_str.decode() if t_test == bytes and t_corpus == str else test_str
        the_test_function = self.assertIn if the_test else self.assertNotIn
        msg_param = "was not found" if the_test else "was found"
        
        try:
            return the_test_function(test_str, corpus)
        except AssertionError:
            if len(corpus) > 1000:
                test_str = test_str.decode() if isinstance(test_str, bytes) else test_str
                raise AssertionError(
                    f"'{test_str}' {msg_param} in the provided text. (The provided text was over "
                    "1000 characters, try self.assertIn or self.assertNotIn for full text of failure."
                ) from None
                # from None suppresses the original stack trace.
            else:
                raise
    
    def _assertEqual(self, first, second, msg=None):
        # we need to be able to bypass our override of assertEqual
        return super().assertEqual(first, second, msg)
    
    def assertEqual(self, first, second, msg=None):
        """ Overrides and inserts our diff_strings func to make the error easy to parse. """
        try:
            return super().assertEqual(first, second, msg)
        except AssertionError:
            # walking the stack is slow, so we do some type checking first
            if not isinstance(first, (bytes, str, datetime, dict)):
                raise
            
            # We need to know if this is inside an assertRaises call because we don't want that
            # # situation to trigger the print stameent.
            # walk stack, look for presence of assertRaises
            found = False
            for frame in traceback.extract_stack():
                if "assertRaises" in frame.name:
                    found = True
                    break
            
            if not found:
                # do type checking first because the stack walking is slow
                if isinstance(first, (bytes, str)) and isinstance(second, (bytes, str)):
                    # inject our nice diff_strings function
                    print("first: ", first)
                    print()
                    print("second:", second)
                    print()
                    diff_strings(first, second)
                
                elif isinstance(first, datetime) and isinstance(second, datetime):
                    print("\nThese times are not equal:")
                    print("first: ", tformat(first))
                    print("second:", tformat(second))
                    print()
                
                elif isinstance(first, dict) and isinstance(second, dict):
                    compare_dictionaries(first, second)
            
            # and then raise
            raise
    
    def assert_researcher_relation(self, researcher: Researcher, study: Study, relationship: str):
        try:
            if relationship == ResearcherRole.site_admin:
                researcher.refresh_from_db()
                self.assertTrue(researcher.site_admin)
                # no relationships because it is a site admin
                self.assertEqual(
                    StudyRelation.objects.filter(study=study, researcher=researcher).count(), 0
                )
            elif relationship is None:
                # Relationship should not exist because it was set to None
                self.assertFalse(
                    StudyRelation.objects.filter(study=study, researcher=researcher).exists()
                )
            elif relationship in REAL_ROLES:
                # relatioship is supposed to be the provided relatioship (researcher or study_admin)
                self.assertEqual(
                    StudyRelation.objects.filter(
                        study=study, researcher=researcher, relationship=relationship).count(),
                    1
                )
            else:
                raise Exception("invalid researcher role provided")
        except AssertionError:
            print("researcher:", researcher.username)
            print("study:", study.name)
            print("relationship that it should be:", relationship)
            real_relatiosnship = StudyRelation.objects.filter(study=study, researcher=researcher)
            if not real_relatiosnship:
                print("relationship was 'None'")
            else:
                print(f"relationship was '{real_relatiosnship.get().relationship}'")
            raise
    
    @staticmethod
    def mutate_variable(var, ignore_bools=False):
        if isinstance(var, bool):
            return var if ignore_bools else not var
        elif isinstance(var, (float, int)):
            return var + 1
        elif isinstance(var, str):
            return var + "aaa"
        else:
            raise TypeError(f"Unhandled type: {type(var)}")
    
    @staticmethod
    def un_mutate_variable(var, ignore_bools=False):
        if isinstance(var, bool):
            return not var if ignore_bools else var
        elif isinstance(var, (float, int)):
            return var - 1
        elif isinstance(var, str):
            if not var.endswith("eee"):
                raise Exception(f"string '{var} was not a mutated variable")
            return var[-3:]
        else:
            raise TypeError(f"Unhandled type: {type(var)}")
    
    def simple_get(self, url: str, status_code=None, **get_kwargs) -> ResponseOrRedirect:
        """ provide a url with, supports a status code check, only get kwargs"""
        ret = self.client.get(url, **get_kwargs)
        if status_code is not None:
            self.assertEqual(status_code, ret.status_code)
        return ret
    
    def easy_get(self, view_name: str, status_code=None, **get_kwargs) -> ResponseOrRedirect:
        """ very easy, use endpoint names, no reverse args only kwargs """
        url = self.smart_reverse(view_name, kwargs=get_kwargs)
        return self.simple_get(url, status_code=status_code, **get_kwargs)
    
    def smart_reverse(self, endpoint_name: str, args: tuple = None, kwargs: dict = None):
        kwargs = {} if kwargs is None else kwargs
        args = {} if args is None else args
        try:
            return reverse(endpoint_name, args=args, kwargs=kwargs)
        except NoReverseMatch:
            if endpoint_name not in ENDPOINTS_BY_NAME:
                raise MisconfiguredTestException(
                    f"'{endpoint_name}' is not an endpoint anywhere, check urls.py"
                ) from None
            raise MisconfiguredTestException(
                f"smart_get_redirect error, bad reverse_params/reverse_kwargs for {endpoint_name}:\n"
                f"pattern: {ENDPOINTS_BY_NAME[endpoint_name].pattern}\n"
                f"reverse_params: {args}\n"
                f"reverse_kwargs: {kwargs}"
            ) from None
    
    def deepdiff(self, d1, d2) -> DeepDiff:
        columns = os.get_terminal_size().columns
        def unrootify(x: dict) -> dict:
            # removes "root[" and "]" from the keys in the dict, as well as ' and "
            if isinstance(x, list):
                for i, v in enumerate(x):
                    if v.startswith("root"):
                        v = v.replace("root", "", 1)
                        x[i] = v
                    if (v.startswith("'") and v.endswith("'")) or (v.startswith('"') and v.endswith('"')):
                        x[i] = v[1:-1]
            if isinstance(x, dict):
                for k in list(x.keys()):
                    if k.startswith("root["):
                        new_k = k.replace("root[", "", 1)[:-1]
                        x[new_k] = x.pop(k)
                for k in list(x.keys()):
                    if (k.startswith("'") and k.endswith("'")) or (k.startswith('"') and k.endswith('"')):
                        new_k = k[1:-1]
                        x[new_k] = x.pop(k)
            return x
        
        print("\n[ deefdiff: ]")
        d = DeepDiff(d1, d2)
        
        if values_changed := d.pop("values_changed", None):
            print("[ values_changed: ]")
            pprint(unrootify(values_changed), width=columns)
        
        if type_changes := d.pop("type_changes", None):
            print("[ type_changes: ]")
            pprint(unrootify(type_changes), width=columns)
        
        if unprocessed:= d.pop("unprocessed"):
            print("[ unprocessed: ]")
            pprint(unrootify(unprocessed), width=columns)
        
        if d:
            pprint(d, width=columns)
        print()
        return d
    
    def dd(self, d1, d2) -> DeepDiff:
        return self.deepdiff(d1, d2)

class BasicSessionTestCase(CommonTestCase):
    """ This class has the basics needed to do login operations, but runs no extra setup before each
    test.  This class is probably only useful to test the login pages. """
    
    def do_default_login(self, **post_params: Dict[str, str]) -> HttpResponse:
        # logs in the default researcher user, assumes it has been instantiated.
        return self.do_login(
            self.DEFAULT_RESEARCHER_NAME, self.DEFAULT_RESEARCHER_PASSWORD, post_params=post_params
        )
    
    def do_login(self, username, password, mfa_code=None, post_params: Dict = None) -> HttpResponse:
        post_params = {} if post_params is None else post_params
        if mfa_code:
            post_params["mfa_code"] = mfa_code
        url = path_join(self.smart_reverse("login_endpoints.validate_login"))
        return self.client.post(url, data={"username": username, "password": password, **post_params})
    
    def do_researcher_logout(self):
        return self.client.get(self.smart_reverse("login_endpoints.logout_page"))


class SmartRequestsTestCase(BasicSessionTestCase):
    """ An ENDPOINT_NAME is a string of the form "file_endpoint_is_in.view_name", for example
    "login_endpoints.validate_login" or "participant_endpoints.participant_page". These tests must also be
    in a test file that mimics the endpoint name, for example test_login_endpoints.py or
    test_participant_endpoints.py. These rules are enforced enforced by the test class as two #
    automatic tests that are tacked on to the end of every test class implemented below.
    
    REDIRECT_ENDPOINT_NAME is identical in form to ENDPOINT_NAME, it is used to make testing an
    exceptionally common pattern trivial. """
    ENDPOINT_NAME = None
    REDIRECT_ENDPOINT_NAME = None
    IGNORE_THIS_ENDPOINT = "ignore this endpoint"  # turns out we need to suppress this sometimes...
    
    # Never add this test to a subclass
    def test_has_valid_endpoint_name_and_is_placed_in_correct_file(self):
        # case: [currently] subclasses that but are intended to be further subclassed are contained
        # in the tests.common, so they can break the rules.
        if self.__class__.__module__ == "tests.common" or self.ENDPOINT_NAME == self.IGNORE_THIS_ENDPOINT:
            return
        
        # Rule: test classes should be in a test file with the same structure module name  as the
        # endpoint they are testing.
        target_according_to_filename = self.__class__.__module__.split(".")[-1].replace("test_", "")
        target_according_to_endpoint_name = self.ENDPOINT_NAME.split(".")[0]
        full_test_class_location = f"{self.__class__.__module__}.{self.__class__.__name__}"
        assert target_according_to_filename == target_according_to_endpoint_name, (
            f"Test class '{full_test_class_location.replace('.', ' . ')}' is in the wrong file.\n\n"
            f"it targets the endpoint '{self.ENDPOINT_NAME}',\nbut "
            f"it is located in    '{self.__class__.__module__}.py'.\n"
            f"It should be in         'tests.test_{target_according_to_endpoint_name}.py\n"
            f"along with all the other endpoint tests for {target_according_to_endpoint_name}."
        )
        
        # Rule: subclasses must have a valid endpoint name or be explicitly set to
        # IGNORE_THIS_ENDPOINT. REDIRECT_ENDPOINT_NAME must be None or a valid endpoint name.
        end_name = self.ENDPOINT_NAME  # using variable names to shorten these because ...
        ignore = self.IGNORE_THIS_ENDPOINT
        r_end_name = self.REDIRECT_ENDPOINT_NAME
        # check endpoints ard redirects are valid
        assert end_name in ENDPOINTS_BY_NAME or end_name == ignore, \
            f"Test class {self.__class__.__name__}'s ENDPOINT_NAME `{end_name}` does not exist."
        
        # assert r_end_name != ignore and (end_name is None or r_end_name not in ENDPOINTS_BY_NAME):
        assert r_end_name == ignore or (r_end_name in ENDPOINTS_BY_NAME or r_end_name is None), \
            f"{self.__class__.__name__}'s REDIRECT_ENDPOINT_NAME {r_end_name}` does not exist."
    
    ## Machinery for ensuring our smart_* methods that require redirects are used correctly.
    def ensure_has_redirect(some_function: Callable):
        """ Wrapped functions must have a class variable REDIRECT_ENDPOINT_NAME populated. """
        @wraps(some_function)
        def checker_func(*args, **kwargs):
            # this signature can't have self declared, but the first arg is always self
            if not args[0].REDIRECT_ENDPOINT_NAME:
                raise ImproperlyConfigured("You must provide a value for REDIRECT_ENDPOINT_NAME.")
            return some_function(*args, **kwargs)
        return checker_func
    
    def validate_status_code(some_function: Callable):
        @wraps(some_function)
        def checker_function(*args, **kwargs):
            status_code = args[1]  # the convention is that the status code is the first arg after self
            if not isinstance(status_code, int):
                raise TypeError(f"received {type(status_code)} '{status_code}' for status_code?")
            if status_code < 200 or status_code > 600:
                raise ImproperlyConfigured(
                    f"'{status_code}' ({type(status_code)}) is definetely not a status code."
                )
            return some_function(*args, **kwargs)
        return checker_function
    
    def smart_post(self, *reverse_args, reverse_kwargs=None, **post_params) -> HttpResponse:
        """ A wrapper to do a post request, using reverse on the ENDPOINT_NAME, and with a
        reasonable pattern for providing parameters to both reverse and post. """
        reverse_kwargs = reverse_kwargs or {}
        self._detect_obnoxious_type_error("smart_post", reverse_args, reverse_kwargs, post_params)
        return self.client.post(
            self.smart_reverse(self.ENDPOINT_NAME, args=reverse_args, kwargs=reverse_kwargs), data=post_params
        )
    
    def smart_get(self, *reverse_params, reverse_kwargs=None, **get_kwargs) -> HttpResponse:
        """ A wrapper to do a get request, using reverse on the ENDPOINT_NAME, and with a reasonable
        pattern for providing parameters to both reverse and get. """
        reverse_kwargs = reverse_kwargs or {}
        # print(f"*reverse_params: {reverse_params}\n**get_kwargs: {get_kwargs}\n**reverse_kwargs: {reverse_kwargs}\n")
        self._detect_obnoxious_type_error("smart_get", reverse_params, reverse_kwargs, get_kwargs)
        url = self.smart_reverse(self.ENDPOINT_NAME, args=reverse_params, kwargs=reverse_kwargs)
        response = self.client.get(url, **get_kwargs)
        
        # if running in v3 mode we run the open-in-browser code
        if VERBOSE_3 and 200 <= response.status_code < 300:
            render_test_html_file(response, url)
        
        return response
    
    @validate_status_code
    def smart_post_status_code(
        self, status_code: int, *reverse_args, reverse_kwargs=None, **post_params
    ) -> HttpResponse:
        """ This helper function takes a status code in addition to post parameters, and tests for
        it.  Use for writing concise tests. """
        # print(f"reverse_args: {reverse_args}\nreverse_kwargs: {reverse_kwargs}\npost_params: {post_params}")
        resp = self.smart_post(*reverse_args, reverse_kwargs=reverse_kwargs, **post_params)
        self.assertEqual(resp.status_code, status_code)
        return resp
    
    def smart_get_status_code(
        self, status_code: int, *reverse_params, reverse_kwargs=None, **get_kwargs
    ) -> HttpResponse:
        """ As smart_get, but tests for a given status code on the response. """
        resp = self.smart_get(*reverse_params, reverse_kwargs=reverse_kwargs, **get_kwargs)
        self.assertEqual(resp.status_code, status_code)
        return resp
    
    @ensure_has_redirect
    def smart_get_redirect(self, *reverse_params, get_kwargs=None, **reverse_kwargs) -> HttpResponseRedirect:
        """ As smart_get, but checks for a redirect. """
        get_kwargs = get_kwargs or {}
        # print(f"*reverse_params: {reverse_params}\n**get_kwargs: {get_kwargs}\n**reverse_kwargs: {reverse_kwargs}\n")
        self._detect_obnoxious_type_error("smart_get_redirect", reverse_params, reverse_kwargs, get_kwargs)
        response = self.smart_get_status_code(
            302, *reverse_params, reverse_kwargs=reverse_kwargs, **get_kwargs
        )
        self.assertIsInstance(response, HttpResponseRedirect)
        self.assertEqual(resolve(response.url).url_name, self.REDIRECT_ENDPOINT_NAME)
        return response
    
    @ensure_has_redirect
    def smart_post_redirect(self, *reverse_args, reverse_kwargs={}, **post_params) -> HttpResponseRedirect:
        # As smart post, but assert that the request was redirected, and that it points to the
        # appropriate endpoint.
        reverse_kwargs = reverse_kwargs or {}
        response = self.smart_post_status_code(
            302, *reverse_args, reverse_kwargs=reverse_kwargs, **post_params
        )
        self.assertIsInstance(response, HttpResponseRedirect)
        self.assertEqual(resolve(response.url).url_name, self.REDIRECT_ENDPOINT_NAME)
        return response
    
    @ensure_has_redirect
    def redirect_get_contents(self, *reverse_params, get_kwargs=None, **reverse_kwargs) -> bytes:
        """Tests frequently need a page to test for content messages.  This method loads the
        REDIRECT_ENDPOINT_NAME page, and returns html content for further checking. """
        get_kwargs = get_kwargs or {}
        # print(f"*reverse_params: {reverse_params}\n**get_kwargs: {get_kwargs}\n**reverse_kwargs: {reverse_kwargs}\n")
        self._detect_obnoxious_type_error("redirect_get_contents", reverse_params, reverse_kwargs, get_kwargs)
        response = self.client.get(
            self.smart_reverse(self.REDIRECT_ENDPOINT_NAME, args=reverse_params, kwargs=reverse_kwargs),
            **get_kwargs
        )
        self.assertEqual(response.status_code, 200)
        return response.content
    
    @staticmethod
    def _detect_obnoxious_type_error(function_name: str, args: tuple, kwargs1: dict, kwargs2: dict):
        for arg in chain(args, kwargs1.values(), kwargs2.values()):
            if isinstance(arg, Model):
                raise TypeError(f"encountered {type(arg)} passed to {function_name}.")


class ResearcherSessionTest(SmartRequestsTestCase):
    """ This class sets up a logged-in researcher user (using the variable name "session_researcher"
    to mimic the convenience variable in the real code).  This is the base test class that all
    researcher endpoints should use. """
    
    def setUp(self) -> None:
        """ Log in the session researcher. """
        self.session_researcher  # populate the session researcher
        self.do_default_login()
        return super().setUp()


class ParticipantSessionTest(SmartRequestsTestCase):
    ENDPOINT_NAME = None
    IOS_ENDPOINT_NAME = None
    DISABLE_CREDENTIALS = False
    DEVICE_TRACKING_FIELDS = ("last_version_code", "last_version_name", "last_os_version", "device_status_report")
    DEVICE_TRACKING_PARAMS = ("version_code", "version_name", "os_version", "device_status_report")
    
    def setUp(self) -> None:
        """ Populate the session participant variable. """
        self.session_participant = self.default_participant
        self.INJECT_DEVICE_TRACKER_PARAMS = True  # reset for every test
        self.INJECT_RECEIVED_SURVEY_UUIDS = True  # reset for every test
        return super().setUp()
    
    def smart_post(self, *reverse_args, reverse_kwargs=None, **post_params) -> HttpResponse:
        """ Injects parameters for authentication and confirms the device tracking fields are 
        tracking. Features can be toggled """
        
        if not self.DISABLE_CREDENTIALS:
            post_params["patient_id"] = self.session_participant.patient_id
            post_params["device_id"] = self.DEFAULT_PARTICIPANT_DEVICE_ID
            # the participant password is special.
            post_params["password"] = self.DEFAULT_PARTICIPANT_PASSWORD_HASHED
        
        if self.INJECT_RECEIVED_SURVEY_UUIDS and "survey_uuids" not in post_params:
            post_params["notification_uuids"] = '["a1019758-63f1-48a9-96bf-08208f2c9055"]'
        
        if self.INJECT_DEVICE_TRACKER_PARAMS:
            for tracking_param in self.DEVICE_TRACKING_PARAMS:
                if tracking_param not in post_params:
                    post_params[tracking_param] = generate_easy_alphanumeric_string(10)
        else:
            orig_vals = Participant.objects.filter(pk=self.session_participant.pk) \
                            .values(*self.DEVICE_TRACKING_FIELDS).get()
        
        ret = super().smart_post(*reverse_args, reverse_kwargs=reverse_kwargs, **post_params)
        tracker_vals = Participant.objects.filter(pk=self.session_participant.pk) \
                        .values(*self.DEVICE_TRACKING_FIELDS).get()
        
        # keep this code explicit or else it becomes unmaintainable
        if self.INJECT_DEVICE_TRACKER_PARAMS:
            self.assertEqual(tracker_vals["last_version_code"], post_params["version_code"],
                             msg="last_version_code did not update")
            self.assertEqual(tracker_vals["last_version_name"], post_params["version_name"],
                             msg="last_version_name did not update")
            self.assertEqual(tracker_vals["last_os_version"], post_params["os_version"],
                             msg="last_os_version did not update")
            self.assertEqual(tracker_vals["device_status_report"], post_params["device_status_report"],
                             msg="device_status_report did not update")
        
        # ensure there is only 1 of these in all situations
        if self.INJECT_RECEIVED_SURVEY_UUIDS:
            self.assertEqual(
                SurveyNotificationReport.objects.filter(  # 
                    notification_uuid="a1019758-63f1-48a9-96bf-08208f2c9055").count(), 1,
                    msg="SurveyNotificationReport count is wrong."
                )
            
        # reset the toggle after every request
        self.INJECT_DEVICE_TRACKER_PARAMS = True
        return ret


class DataApiTest(SmartRequestsTestCase):
    DISABLE_CREDENTIALS = False
    API_KEY: ApiKey = None
    
    def setUp(self) -> None:
        self.API_KEY = ApiKey.generate(self.session_researcher)
        self.session_access_key = self.API_KEY.access_key_id
        self.session_secret_key = self.API_KEY.access_key_secret_plaintext
        return super().setUp()
    
    def smart_post(self, *reverse_args, reverse_kwargs={}, **post_params) -> HttpResponseRedirect:
        if not self.DISABLE_CREDENTIALS:
            # As smart post, but assert that the request was redirected, and that it points to the
            # appropriate endpoint.
            post_params["access_key"] = self.session_access_key
            post_params["secret_key"] = self.session_secret_key
        return super().smart_post(*reverse_args, reverse_kwargs=reverse_kwargs, **post_params)
    
    def smart_post_status_code(
        self, status_code: int, *reverse_args, reverse_kwargs=None, **post_params
    ) -> HttpResponse:
        """ We need to inject the session keys into the post parameters because the code that uses
        smart_post is inside the super class. """
        if not self.DISABLE_CREDENTIALS:
            post_params["access_key"] = self.session_access_key
            post_params["secret_key"] = self.session_secret_key
        return super().smart_post_status_code(
            status_code, *reverse_args, reverse_kwargs=reverse_kwargs, **post_params
        )
    
    def less_smart_post(self, *reverse_args, reverse_kwargs=None, **post_params) -> HttpResponse:
        """ we need the passthrough and calling super() in an implementation class is dumb.... """
        return super().smart_post(*reverse_args, reverse_kwargs=reverse_kwargs, **post_params)


class TableauAPITest(ResearcherSessionTest):
    
    @property
    def default_header(self):
        # this object is in place of a request object, all we need is a populated .headers attribute
        class NotRequest:
            headers = {
                X_ACCESS_KEY_ID: self.api_key_public,
                X_ACCESS_KEY_SECRET: self.api_key_private,
            }
        return NotRequest
    
    @property
    def raw_headers(self):
        # in http-land a header is distinguished from other kinds of parameters by the prefixing
        # of an all-caps HTTP_.  Go figure.
        return {
            f"HTTP_{X_ACCESS_KEY_ID}": self.api_key_public,
            f"HTTP_{X_ACCESS_KEY_SECRET}": self.api_key_private,
        }
    
    def setUp(self) -> None:
        ret = super().setUp()
        self.api_key = ApiKey.generate(self.session_researcher)
        self.api_key_public = self.api_key.access_key_id
        self.api_key_private = self.api_key.access_key_secret_plaintext
        self.set_session_study_relation(ResearcherRole.researcher)
        return ret
