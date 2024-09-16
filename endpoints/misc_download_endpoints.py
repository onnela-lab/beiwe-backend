import uuid

from django.http.request import HttpRequest
from django.shortcuts import redirect

from authentication.admin_authentication import authenticate_researcher_login
from config.settings import DOWNLOADABLE_APK_URL
from libs.internal_types import ResearcherRequest


"""##### Methods responsible for distributing APK file of Android app. #####"""

# these download app version urls are redirects, which get cached in the browser.  The uniqueify
# parameter is used to make the url look unique, so the browser uses a new url every time.

def download_current(request: ResearcherRequest):
    return redirect(DOWNLOADABLE_APK_URL + "?uniqueify=" + str(uuid.uuid4()))


@authenticate_researcher_login
def download_current_debug(request: ResearcherRequest):
    # add a uuid value to the end so the link always works
    return redirect("https://s3.amazonaws.com/beiwe-app-backups/debug/Beiwe-debug.apk?uniqueify=" + str(uuid.uuid4()))


@authenticate_researcher_login
def download_beta(request: ResearcherRequest):
    return redirect("https://s3.amazonaws.com/beiwe-app-backups/release/Beiwe.apk?uniqueify=" + str(uuid.uuid4()))


@authenticate_researcher_login
def download_beta_debug(request: ResearcherRequest):
    return redirect("https://s3.amazonaws.com/beiwe-app-backups/debug/Beiwe-debug.apk?uniqueify=" + str(uuid.uuid4()))


@authenticate_researcher_login
def download_beta_release(request: ResearcherRequest):
    return redirect("https://s3.amazonaws.com/beiwe-app-backups/release/Beiwe-2.2.3-onnelaLabServer-release.apk?uniqueify=" + str(uuid.uuid4()))


def download_privacy_policy(request: HttpRequest):
    return redirect("https://s3.amazonaws.com/beiwe-app-backups/Beiwe+Data+Privacy+and+Security.pdf")
