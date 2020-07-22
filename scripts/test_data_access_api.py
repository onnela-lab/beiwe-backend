from os.path import abspath
from sys import path
path.insert(0, abspath(__file__).rsplit('/', 2)[0])

import itertools
import requests

from config.constants import ResearcherRole
from pprint import pprint
from data_access_api_reference import download_data
from database.study_models import Study
from database.user_models import Researcher, StudyRelation

try:
    test_user = Researcher.objects.get(username="test_user")
except Researcher.DoesNotExist:
    test_user = Researcher.create_without_password("test_user")

download_data.API_URL_BASE = "http://127.0.0.1:8080/"
debugging_study = Study.objects.get(name='debugging study')


download_data.RUNNING_IN_TEST_MODE = True
download_data.SKIP_DOWNLOAD = True

def helper(
        allowed_on_study, corrupt_access_id, corrupt_secret_key, researcher_admin, site_admin,
        batch_user, study_as_object_id, wrong_access_key, wrong_secret_key, is_test_study,
        corrupt_study_object_id
):
    if not study_as_object_id and corrupt_study_object_id:
        # invalid test scenario, skip
        return

    access_key, secret_key = test_user.reset_access_credentials()
    test_user.study_relations.all().delete()
    test_user.site_admin = site_admin
    test_user.is_batch_user = batch_user
    test_user.save()

    # set test study flag
    Study.objects.filter(pk=debugging_study.pk).update(is_test=is_test_study)

    if not site_admin and not batch_user:
        # regular_user
        relationship = ResearcherRole.study_admin if researcher_admin else ResearcherRole.researcher
        if allowed_on_study:
            StudyRelation.objects.get_or_create(
                study_id=debugging_study.pk, researcher_id=test_user.pk, relationship=relationship
            )

    # stick a disallowed character in the key, end is fine
    if corrupt_access_id:
        access_key = access_key[-2] + "@"
    if corrupt_secret_key:
        secret_key = secret_key[-2] + "@"

    if wrong_access_key:
        access_key = "jeff"
    if wrong_secret_key:
        secret_key = "jeff"

    if study_as_object_id:
        if corrupt_study_object_id:
            study_id = ("@@@@@@@")
        else:
            study_id = debugging_study.object_id
    else:
        study_id = debugging_study.pk

    print("USING:")
    print("\tstudy_id: ", study_id)
    print("\taccess_key: ", access_key)
    print("\tsecret_key: ", secret_key)
    print()
    download_data.make_request(
        study_id,
        access_key=access_key,
        secret_key=secret_key,
        # set end date to an impossible value so we don't actually download data.
        time_end="1900-01-31T07:30:04"
    )


# this gets the permutations of all possible contents of booleans
all_possible_combinations = [
    tuple(i) for i in itertools.product((True, False),repeat=helper.__code__.co_argcount)
]
variable_names = helper.__code__.co_varnames

for allowed_on_study, corrupt_access_id, corrupt_secret_key, researcher_admin, site_admin, \
    batch_user, study_as_object_id, wrong_access_key, wrong_secret_key, is_test_study, \
    corrupt_study_object_id in all_possible_combinations:

    print("\n=======================================================================\n")

    kwargs = {
        "allowed_on_study": allowed_on_study,
        "corrupt_access_id": corrupt_access_id,
        "corrupt_secret_key": corrupt_secret_key,
        "researcher_admin": researcher_admin,  # not tested below, present to test false negatives.
        "site_admin": site_admin,
        "batch_user": batch_user,
        "study_as_object_id": study_as_object_id,
        "wrong_access_key": wrong_access_key,
        "wrong_secret_key": wrong_secret_key,
        "is_test_study": is_test_study,
        "corrupt_study_object_id": corrupt_study_object_id,
    }
    assert len(kwargs) == helper.__code__.co_argcount

    # cases:
    # 200
    #   should not occur if any of the access creds are wrong
    #   should only occur if the user is a site admin, the batch user or a researcher assigned to the study

    # 400 -
    #   corrupt credentials
    #   no such user?

    # 403 -
    #   wrong credentials
    #   no such user?

    #  404 -
    #   no such study

    try:
        helper(
            allowed_on_study, corrupt_access_id, corrupt_secret_key, researcher_admin, site_admin,
            batch_user, study_as_object_id, wrong_access_key, wrong_secret_key, is_test_study,
            corrupt_study_object_id
        )
    except requests.exceptions.HTTPError as e:
        error_code = int(str(e))
        print(f"error: {error_code}\n")
        for k, v in kwargs.items():
            print(f'\t {v}: {k}')
        print()

        if error_code == 200:
            # determine that none of the obvious failures are present, and that they should have access.
            assert not corrupt_access_id, "200: corrupt_access_id..."
            assert not corrupt_secret_key, "200: corrupt_secret_key..."
            assert not wrong_access_key, "200: wrong_access_key..."
            assert not wrong_secret_key, "200: wrong_secret_key..."
            assert any((is_test_study, batch_user, site_admin)), "200: shouldn't be allowed on study 1"
            # needs to be allowed and study acceptable
            if not batch_user and not site_admin:
                assert allowed_on_study, "200: should be allowed on study 2"
                assert is_test_study, "200: should be allowed on study 3"

        if error_code == 400:
            # this means there was a _problem with the request_, which only happens with _bad_
            # parameters, not _wrong_ parameters
            assert (corrupt_access_id or corrupt_secret_key or corrupt_study_object_id)

        if error_code == 403:
            # wrong secret and not allowed are obvious, wrong access key because we can't
            # tell malicious caller that a user doesn't exist.
            assert wrong_secret_key or not allowed_on_study or wrong_access_key

        if error_code == 404:
            # wrong identifiers result in 404s, and studies that exist but are not test studies.
            assert wrong_access_key or not is_test_study

        if error_code in (404, 403) and not wrong_access_key and not wrong_secret_key:
            # provided access and secret key are correct 404s and 403s should never happen to special users
            assert not site_admin, f"{error_code}, site admin should have global access."
            assert not batch_user, f"{error_code}, batch_user should have global access."

