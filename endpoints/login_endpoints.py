import bleach
from django.contrib import messages
from django.http.request import HttpRequest
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.http import require_GET, require_POST

from authentication import admin_authentication
from constants.message_strings import (MFA_CODE_6_DIGITS, MFA_CODE_DIGITS_ONLY, MFA_CODE_MISSING,
    MFA_CODE_WRONG)
from database.user_models_researcher import Researcher
from libs.internal_types import ResearcherRequest
from libs.utils.security_utils import verify_mfa


@require_GET
def login_page(request: HttpRequest):
    # if they are logged in then / redirects to the choose study page.
    if admin_authentication.check_is_logged_in(request):
        return redirect("/choose_study")
    
    # Django automatically de-urlifies GET parameters. All our urls are supposed to be unescaped
    # url-safe strings, so if there was any de-urlifying we immediately reject it.  In addition
    # we pass the string through ~regex matches on valid url schemes.
    final_referrer = None
    de_urlified_referrer = request.GET.get('page', "")
    raw_referrer = request.get_full_path().replace("/?page=", "", 1)
    
    if de_urlified_referrer == raw_referrer:
        if admin_authentication.determine_redirectable(de_urlified_referrer):
            final_referrer = de_urlified_referrer
    
    # and finally, final_referrer is then also passed through the global sanitization filter when
    # embedded as the hidden value on the page.
    return render(request, 'admin_login.html', context={"redirect_page": final_referrer})


@require_GET
def logout_page(request: ResearcherRequest):
    """ Technically not a page, but its a url target that clears session information for a researcher. """
    admin_authentication.logout_researcher(request)
    return redirect("/")


@require_POST
def validate_login(request: HttpRequest):
    """ Authenticates administrator login, redirects to login page if authentication fails. """
    username = request.POST.get("username", None)
    password = request.POST.get("password", None)
    mfa_code = request.POST.get("mfa_code", "")
    
    # Test password, username, if the don't match return to login page.
    # Known bug, 2023-12-24 - this endpoint threw
    # "ValidationError: {'password_min_length': ['Ensure this value is greater than or equal to 8.']}""
    # on Researcher.check_password. Was unable to reproduce. test_password_too_short_bad_state tests
    # that min_password_length at least results in a redirect to the manage credentials page on a
    # successful password challenge with that state, but does not trigger such an error.
    if not (username and password and Researcher.check_password(username, password)):
        messages.warning(request, "Incorrect username & password combination; try again.")
        return redirect(reverse("login_endpoints.login_page"))
    
    # login has succeeded, researcher is safe to get
    researcher = Researcher.objects.get(username=username)
    
    # case: mfa is not required, but was provided
    if not researcher.mfa_token and mfa_code:
        messages.warning(request, "MFA code was not required.")
    
    # cases: mfa was missing, bad length, non-digits. Display message, only mfa missing requires
    # a return to the login page, other cases are guaranteed to fail mfa check.
    if researcher.mfa_token and mfa_code and len(mfa_code) != 6:
        messages.warning(request, MFA_CODE_6_DIGITS)
    if researcher.mfa_token and mfa_code and not mfa_code.isdecimal():
        messages.warning(request, MFA_CODE_DIGITS_ONLY)
    if researcher.mfa_token and not mfa_code:
        messages.error(request, MFA_CODE_MISSING)
        return redirect(reverse("login_endpoints.login_page"))
    
    # case: mfa is required, was provided, but was incorrect.
    if researcher.mfa_token and mfa_code and not verify_mfa(researcher.mfa_token, mfa_code):
        messages.error(request, MFA_CODE_WRONG)
        return redirect(reverse("login_endpoints.login_page"))
    
    # case: mfa is required, was provided, and was correct.
    # The redirect happens even when credentials need to be updated, any further levels of
    # redirection occur after the browser follows the redirect and hits the logic in
    # admin_authentication.py.
    admin_authentication.log_in_researcher(request, username)
    
    # Now we try to redirect. First we check the post parameter referrer page for validity, then to
    # the most recent page, then the choose study page. We test for any changes to the referrer url
    # from the page as it is injectable, if there was we skip it.
    referrer_page = request.POST.get('referrer', "")
    if bleach.clean(referrer_page, strip=True) == referrer_page:
        if admin_authentication.determine_redirectable(referrer_page):
            return redirect(referrer_page if referrer_page.startswith("/") else "/" + referrer_page)
    redirect_page = researcher.most_recent_page or ""
    if admin_authentication.determine_redirectable(redirect_page):
        return redirect(redirect_page if redirect_page.startswith("/") else "/" + redirect_page)
    return redirect("/choose_study")
