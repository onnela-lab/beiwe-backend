from datetime import datetime, timedelta

from django.contrib import messages
from django.core.exceptions import ValidationError
from django.http.response import HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_http_methods, require_POST
from markupsafe import Markup

from authentication.admin_authentication import (assert_admin, assert_researcher_under_admin,
    authenticate_admin, authenticate_researcher_login)
from config.settings import DOMAIN_NAME
from constants.message_strings import (API_KEY_IS_DISABLED, API_KEY_NOW_DISABLED, MFA_CODE_6_DIGITS,
    MFA_CODE_DIGITS_ONLY, MFA_CODE_MISSING, MFA_RESET_BAD_PERMISSIONS, MFA_SELF_BAD_PASSWORD,
    MFA_SELF_DISABLED, MFA_SELF_NO_PASSWORD, MFA_SELF_SUCCESS, MFA_TEST_DISABLED, MFA_TEST_FAIL,
    MFA_TEST_SUCCESS, NEW_API_KEY_MESSAGE, NEW_PASSWORD_MISMATCH, NEW_PASSWORD_N_LONG,
    NO_MATCHING_API_KEY, PASSWORD_RESET_FAIL_SITE_ADMIN, PASSWORD_RESET_SUCCESS,
    WRONG_CURRENT_PASSWORD)
from constants.security_constants import MFA_CREATED
from constants.user_constants import EXPIRY_NAME, ResearcherRole
from database.security_models import ApiKey
from database.study_models import Study
from database.user_models_researcher import Researcher, StudyRelation
from libs.django_forms.forms import DisableApiKeyForm, NewApiKeyForm
from libs.endpoint_helpers.password_validation_helpers import (check_password_requirements,
    get_min_password_requirement)
from libs.endpoint_helpers.researcher_helpers import get_administerable_researchers
from libs.endpoint_helpers.study_helpers import get_administerable_studies_by_name
from libs.endpoint_helpers.system_admin_helpers import mfa_clear_allowed
from libs.internal_types import ResearcherRequest
from libs.utils.http_utils import easy_url
from libs.utils.security_utils import create_mfa_object, qrcode_bas64_png, verify_mfa
from middleware.abort_middleware import abort


#
## Administrator Management
#

@require_GET
@authenticate_admin
def administrator_manage_researchers_page(request: ResearcherRequest):
    # get the study names that each user has access to, but only those that the current admin  also
    # has access to.
    if request.session_researcher.site_admin:
        session_ids = Study.objects.exclude(deleted=True).values_list("id", flat=True)
    else:
        session_ids = request.session_researcher.\
            study_relations.filter(study__deleted=False).values_list("study__id", flat=True)
    
    researcher_list = []
    for researcher in get_administerable_researchers(request):
        allowed_studies = Study.get_all_studies_by_name().filter(
            study_relations__researcher=researcher, study_relations__study__in=session_ids,
        ).values_list('name', flat=True)
        researcher_list.append(
            ({'username': researcher.username, 'id': researcher.id}, list(allowed_studies))
        )
    return render(request, 'manage_researchers.html', context=dict(admins=researcher_list))


@require_http_methods(['GET', 'POST'])
@authenticate_admin
def administrator_edit_researcher_page(request: ResearcherRequest, researcher_pk: int):
    """ The page and various permissions logic for the edit researcher page. """
    session_researcher = request.session_researcher
    edit_researcher = Researcher.objects.get(pk=researcher_pk)
    
    # site admins can force a password reset on study admins, but not other site admins
    editable_password =\
        not edit_researcher.username == session_researcher.username and not edit_researcher.site_admin
    
    # if the session researcher is not a site admin then we need to restrict password editing
    # to only researchers that are not study_admins anywhere.
    if not session_researcher.site_admin:
        editable_password = editable_password and not edit_researcher.is_study_admin()
    
    # edit_study_info is a list of tuples of (study relationship, whether that study is editable by
    # the current session admin, and the study itself.)
    visible_studies = session_researcher.get_visible_studies_by_name()
    if edit_researcher.site_admin:
        # if the session admin is a site admin then we can skip the complex logic
        edit_study_info = [("Site Admin", True, study) for study in visible_studies]
    else:
        # When the session admin is just a study admin then we need to determine if the study that
        # the session admin can see is also one they are an admin on so we can display buttons.
        administerable_studies = set(get_administerable_studies_by_name(request).values_list("pk", flat=True))
        
        # We need the overlap of the edit_researcher studies with the studies visible to the session
        # admin, and we need those relationships for display purposes on the page.
        edit_study_relationship_map = {
            study_id: relationship.replace("_", " ").title()
            for study_id, relationship in edit_researcher.study_relations
                .filter(study__in=visible_studies).values_list("study_id", "relationship")
        }
        # get the relevant studies, populate with relationship, editability, and the study.
        edit_study_info = [
            (edit_study_relationship_map[study.id], study.id in administerable_studies, study)
            for study in visible_studies.filter(pk__in=edit_study_relationship_map.keys())
        ]
    
    return render(
        request, 'edit_researcher.html',
        dict(
            edit_researcher=edit_researcher,
            edit_study_info=edit_study_info,
            all_studies=get_administerable_studies_by_name(request),
            editable_password=editable_password,
            editable_mfa=mfa_clear_allowed(session_researcher, edit_researcher),
            redirect_url=easy_url('manage_researcher_endpoints.administrator_edit_researcher_page', researcher_pk),
            is_self=edit_researcher.id == session_researcher.id,
        )
    )


@require_POST
@authenticate_admin
def administrator_reset_researcher_mfa(request: ResearcherRequest, researcher_id: int):
    # TODO: actually build and test this
    researcher = get_object_or_404(Researcher, pk=researcher_id)
    
    if mfa_clear_allowed(request.session_researcher, researcher):
        researcher.clear_mfa()
        messages.warning(request, f"MFA token cleared for researcher {researcher.username}.")
    else:
        messages.warning(request, MFA_RESET_BAD_PERMISSIONS)
        return abort(403)
    return redirect(easy_url('manage_researcher_endpoints.administrator_edit_researcher_page', researcher_id))


@require_POST
@authenticate_admin
def administrator_elevate_researcher_to_study_admin(request: ResearcherRequest):
    researcher_pk = request.POST.get("researcher_id", None)
    # some extra validation on the researcher id
    try:
        int(researcher_pk)
    except ValueError:
        return abort(400)
    
    study_pk = request.POST.get("study_id", None)
    assert_admin(request, study_pk)
    edit_researcher = get_object_or_404(Researcher, pk=researcher_pk)
    study = get_object_or_404(Study, pk=study_pk)
    assert_researcher_under_admin(request, edit_researcher, study)
    if edit_researcher.site_admin:
        return abort(403)
    StudyRelation.objects.filter(researcher=edit_researcher, study=study) \
        .update(relationship=ResearcherRole.study_admin)
    
    return redirect(
        request.POST.get("redirect_url", None) or f'/edit_researcher/{researcher_pk}'
    )


@require_POST
@authenticate_admin
def administrator_demote_study_administrator_to_researcher(request: ResearcherRequest):
    # FIXME: this endpoint does not test for site admin cases correctly, the test passes but is
    # wrong. Behavior is fine because it has no relevant side effects except for the know bug where
    # site admins need to be manually added to a study before being able to download data.
    researcher_pk = request.POST.get("researcher_id")
    study_pk = request.POST.get("study_id")
    assert_admin(request, study_pk)
    # assert_researcher_under_admin() would fail here...
    StudyRelation.objects.filter(
        researcher=Researcher.objects.get(pk=researcher_pk),
        study=Study.objects.get(pk=study_pk),
    ).update(relationship=ResearcherRole.researcher)
    return redirect(request.POST.get("redirect_url", None) or f'/edit_researcher/{researcher_pk}')


@require_http_methods(['GET', 'POST'])
@authenticate_admin
def administrator_create_new_researcher(request: ResearcherRequest):
    # FIXME: get rid of dual endpoint pattern, it is a bad idea.
    if request.method == 'GET':
        return render(request, 'create_new_researcher.html')
    
    # Drop any whitespace or special characters from the username (restrictive, alphanumerics-only)
    username = ''.join(c for c in request.POST.get('admin_id', '') if c.isalnum())
    password = request.POST.get('password', '')
    
    if Researcher.objects.filter(username=username).exists():
        messages.error(request, f"There is already a researcher with username {username}")
        return redirect('/create_new_researcher')
    
    if len(password) < 8:
        messages.error(request, NEW_PASSWORD_N_LONG.format(length=8))
        return redirect('/create_new_researcher')
    else:
        researcher = Researcher.create_with_password(username, password)
    return redirect(f'/edit_researcher/{researcher.pk}')


@require_POST
@authenticate_admin
def administrator_add_researcher_to_study(request: ResearcherRequest):
    researcher_id = request.POST['researcher_id']
    study_id = request.POST['study_id']
    assert_admin(request, study_id)
    try:
        StudyRelation.objects.get_or_create(
            study_id=study_id, researcher_id=researcher_id, relationship=ResearcherRole.researcher
        )
    except ValidationError:
        # handle case of the study id + researcher already existing
        pass
    
    # This gets called by both edit_researcher and edit_study, so the POST request
    # must contain which URL it came from.
    # TODO: don't source the url from the page, give it a required post parameter for the redirect and check against that
    return redirect(request.POST['redirect_url'])


@require_POST
@authenticate_admin
def administrator_remove_researcher_from_study(request: ResearcherRequest):
    researcher_id = request.POST['researcher_id']
    study_id = request.POST['study_id']
    try:
        researcher = Researcher.objects.get(pk=researcher_id)
    except Researcher.DoesNotExist:
        return HttpResponse(content="", status=404)
    assert_admin(request, study_id)
    assert_researcher_under_admin(request, researcher, study_id)
    StudyRelation.objects.filter(study_id=study_id, researcher_id=researcher_id).delete()
    # TODO: don't source the url from the page, give it a required post parameter for the redirect and check against that
    return redirect(request.POST['redirect_url'])


@require_GET
@authenticate_admin
def administrator_delete_researcher(request: ResearcherRequest, researcher_id):
    # only site admins can delete researchers from the system.
    if not request.session_researcher.site_admin:
        return HttpResponse(content="", status=403)
    researcher = get_object_or_404(Researcher, pk=researcher_id)
    
    StudyRelation.objects.filter(researcher=researcher).delete()
    researcher.delete()
    return redirect('/manage_researchers')


@require_POST
@authenticate_admin
def administrator_set_researcher_password(request: ResearcherRequest):
    """ This is the endpoint that an admin uses to set another researcher's password.
    This endpoint accepts any value as long as it is 8 characters, but puts the researcher into a
    forced password reset state. """
    researcher = Researcher.objects.get(pk=request.POST.get('researcher_id', None))
    assert_researcher_under_admin(request, researcher)
    if researcher.site_admin:
        messages.warning(request, PASSWORD_RESET_FAIL_SITE_ADMIN)
        return redirect(f'/edit_researcher/{researcher.pk}')
    new_password = request.POST.get('password', '')
    if len(new_password) < 8:
        messages.warning(request, NEW_PASSWORD_N_LONG.format(length=8))
    else:
        researcher.set_password(new_password)
        researcher.update(password_force_reset=True)
        researcher.force_global_logout()
    return redirect(f'/edit_researcher/{researcher.pk}')


#
## Self Management
#


@authenticate_researcher_login
def self_manage_credentials_page(request: ResearcherRequest):
    """ The manage credentials page has two modes of access, one with a password and one without.
    When loaded with the password the MFA code's image is visible. There is also a special
    MFA_CREATED value in the session that forces the QR code to be visible without a password for
    one minute after it was created (based on page-load time). """
    
    # api key names for the researcher - these are sanitized by the template layer.
    api_keys = list(
        request.session_researcher.api_keys
        .filter(is_active=True)  # don't actually need is_active anymore, we are filtering on it.
        .order_by("-created_on").values(
        "access_key_id", "is_active", "readable_name", "created_on"
    ))
    for key in api_keys:
        key["created_on"] = key["created_on"].date().isoformat()
    
    password = request.POST.get("view_mfa_password", None)
    provided_password = password is not None
    password_correct = request.session_researcher.validate_password(password or "")
    has_mfa = request.session_researcher.mfa_token is not None
    mfa_created = request.session.get(MFA_CREATED, False)
    
    # check whether mfa_created occurred in the last 60 seconds, otherwise clear it.
    if isinstance(mfa_created, datetime) and (timezone.now() - mfa_created).total_seconds() > 60:
        del request.session[MFA_CREATED]
        mfa_created = False
    
    # mfa_created is a datetime which is non-falsey.
    if has_mfa and (mfa_created or password_correct):
        obj = create_mfa_object(request.session_researcher.mfa_token.strip("="))
        mfa_url = obj.provisioning_uri(name=request.session_researcher.username, issuer_name=DOMAIN_NAME)
        mfa_png = qrcode_bas64_png(mfa_url)
    else:
        mfa_png = None
    
    return render(
        request,
        'manage_credentials.html',
        context=dict(
            is_admin=request.session_researcher.is_an_admin(),
            api_keys=api_keys,
            new_api_access_key=request.session.pop("generate_api_key_id", None),
            new_api_secret_key=request.session.pop("new_api_secret_key", None),
            min_password_length=get_min_password_requirement(request.session_researcher),
            mfa_png=mfa_png,
            has_mfa=has_mfa,
            display_bad_password=provided_password and not password_correct,
            researcher=request.session_researcher,
        )
    )


@require_POST
@authenticate_researcher_login
def self_reset_mfa(request: ResearcherRequest):
    """ Endpoint either enables and creates a new, or clears the MFA toke for the researcher. 
    Sets a MFA_CREATED value in the session to force the QR code to be visible for one minute. """
    # requires a password to change the mfa setting, basic error checking.
    password = request.POST.get("mfa_password", None)
    if not password:
        messages.error(request, MFA_SELF_NO_PASSWORD)
        return redirect(easy_url("manage_researcher_endpoints.self_manage_credentials_page"))
    if not request.session_researcher.validate_password(password):
        messages.error(request, MFA_SELF_BAD_PASSWORD)
        return redirect(easy_url("manage_researcher_endpoints.self_manage_credentials_page"))
    
    # presence of a "disable" key in the post data to distinguish between setting and clearing.
    # manage adding to or removing MFA_CREATED from the session data.
    if "disable" in request.POST:
        messages.warning(request, MFA_SELF_DISABLED)
        if MFA_CREATED in request.session:
            del request.session[MFA_CREATED]
        request.session_researcher.clear_mfa()
    else:
        messages.warning(request, MFA_SELF_SUCCESS)
        request.session[MFA_CREATED] = timezone.now()
        request.session_researcher.reset_mfa()
    return redirect(easy_url("manage_researcher_endpoints.self_manage_credentials_page"))


@require_POST
@authenticate_researcher_login
def self_test_mfa(request: ResearcherRequest):
    """ endpoint to test your mfa code without accidentally locking yourself out. """
    if not request.session_researcher.mfa_token:
        messages.error(request, MFA_TEST_DISABLED)
        return redirect(easy_url("manage_researcher_endpoints.self_manage_credentials_page"))
    
    mfa_code = request.POST.get("mfa_code", None)
    if mfa_code and len(mfa_code) != 6:
        messages.error(request, MFA_CODE_6_DIGITS)
    if mfa_code and not mfa_code.isdecimal():
        messages.error(request, MFA_CODE_DIGITS_ONLY)
    if not mfa_code:
        messages.error(request, MFA_CODE_MISSING)
    
    # case: mfa is required, was provided, but was incorrect.
    if verify_mfa(request.session_researcher.mfa_token, mfa_code):
        messages.success(request, MFA_TEST_SUCCESS)
    else:
        messages.error(request, MFA_TEST_FAIL)
    
    return redirect(easy_url("manage_researcher_endpoints.self_manage_credentials_page"))


@require_POST
@authenticate_researcher_login
def self_change_password(request: ResearcherRequest):
    try:
        current_password = request.POST['current_password']
        new_password = request.POST['new_password']
        confirm_new_password = request.POST['confirm_new_password']
    except KeyError:
        return abort(400)
    
    if not Researcher.check_password(request.session_researcher.username, current_password):
        messages.warning(request, WRONG_CURRENT_PASSWORD)
        return redirect('manage_researcher_endpoints.self_manage_credentials_page')
    
    success, msg = check_password_requirements(request.session_researcher, new_password)
    if msg:
        messages.warning(request, msg)
    if not success:
        return redirect("manage_researcher_endpoints.self_manage_credentials_page")
    if new_password != confirm_new_password:
        messages.warning(request, NEW_PASSWORD_MISMATCH)
        return redirect('manage_researcher_endpoints.self_manage_credentials_page')
    
    # this is effectively sanitized by the hash operation
    request.session_researcher.set_password(new_password)
    request.session_researcher.update(password_force_reset=False)
    messages.warning(request, PASSWORD_RESET_SUCCESS)
    # expire the session so that the user has to log in again with the new password. (Ve have a
    # feature over in handle_session_expiry in admin_authentication that will block the session
    # period from being extended again if the timeout is within 10 seconds of expiring.)
    request.session[EXPIRY_NAME] = timezone.now() + timedelta(seconds=5)
    return redirect('manage_researcher_endpoints.self_manage_credentials_page')


@require_POST
@authenticate_researcher_login
def self_generate_api_key(request: ResearcherRequest):
    form = NewApiKeyForm(request.POST)
    if not form.is_valid():
        return redirect("manage_researcher_endpoints.self_manage_credentials_page")
    
    api_key = ApiKey.generate(
        researcher=request.session_researcher,
        readable_name=form.cleaned_data['readable_name'],
    )
    request.session["generate_api_key_id"] = api_key.access_key_id
    request.session["new_api_secret_key"] = api_key.access_key_secret_plaintext
    messages.warning(request, Markup(NEW_API_KEY_MESSAGE))
    return redirect("manage_researcher_endpoints.self_manage_credentials_page")


@require_POST
@authenticate_researcher_login
def self_disable_api_key(request: ResearcherRequest):
    form = DisableApiKeyForm(request.POST)
    if not form.is_valid():
        return redirect("manage_researcher_endpoints.self_manage_credentials_page")
    api_key_id = request.POST["api_key_id"]
    api_key_query = ApiKey.objects.filter(access_key_id=api_key_id) \
        .filter(researcher=request.session_researcher)
    
    if not api_key_query.exists():
        messages.warning(request, Markup(NO_MATCHING_API_KEY))
        return redirect("manage_researcher_endpoints.self_manage_credentials_page")
    
    api_key = api_key_query[0]
    if not api_key.is_active:
        messages.warning(request, API_KEY_IS_DISABLED + f" {api_key_id}")
        return redirect("manage_researcher_endpoints.self_manage_credentials_page")
    
    api_key.is_active = False
    api_key.save()
    messages.success(request, API_KEY_NOW_DISABLED.format(key=api_key.access_key_id))
    return redirect("manage_researcher_endpoints.self_manage_credentials_page")
