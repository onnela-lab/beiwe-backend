from typing import Dict

from django.conf import settings
from django.conf.urls.static import static
from django.core.exceptions import ImproperlyConfigured
from django.urls import path as simplepath

from api import (admin_api, copy_study_api, dashboard_api, data_access_api, mobile_api,
    other_data_apis, participant_administration, push_notifications_api, study_api, survey_api,
    tableau_api)
from config.settings import ENABLE_EXPERIMENTS
from constants.common_constants import RUNNING_TESTS
from constants.url_constants import (IGNORE, LOGIN_REDIRECT_IGNORE, LOGIN_REDIRECT_SAFE, SAFE,
    urlpatterns)
from pages import (admin_pages, data_access_web_form, forest_pages, login_pages, mobile_pages,
    participant_pages, survey_designer, system_admin_pages)


def path(
    route: str, view: callable, name: str = None, kwargs: Dict = None, login_redirect: str = None
):
    """ Helper function, for creating url endpoints, automates our common logic across all urls.
     
    We want to automatically append a "/" to all urlpatterns.
    
    If no name is provided we insert the module and function name as the name, e.g.
    "module_name.function_name" like "login_pages.login_page"
    
    login_redirect can be None, IGNORE, or SAFE.
    IGNORE means that the user will never be redirected AWAY from that page (except to the login page).
    SAFE means that the user can be redirected to that url
    None means it is an invalid login-redirect page."""
    
    if name is None:
        name = view.__module__.rsplit(".", 1)[-1] + "." + view.__name__
    
    route_with_slash = route if route.endswith("/") else route + "/"
    route_without_slash = route if not route.endswith("/") else route.rstrip("/")
    
    url_with_slash = simplepath(route_with_slash, view, name=name, kwargs=kwargs)
    url_without_slash = simplepath(route_without_slash, view, name=name, kwargs=kwargs)
    
    urlpatterns.append(url_with_slash)
    urlpatterns.append(url_without_slash)
    
    if login_redirect == IGNORE:
        LOGIN_REDIRECT_IGNORE.append(url_with_slash)
        LOGIN_REDIRECT_IGNORE.append(url_without_slash)
    elif login_redirect == SAFE:
        LOGIN_REDIRECT_SAFE.append(url_with_slash)
        LOGIN_REDIRECT_SAFE.append(url_without_slash)
    elif login_redirect is not None:
        raise ImproperlyConfigured(f"Invalid login_redirect value: {login_redirect}")


# Paths with login_redirect=IGNORE will never be forced to redirect because otherwise the user
# would, for example, be unable to reset their password.

# Paths with login_redirect=SAFE are allowed to be redirected to from the login page based on prior
# researcher user activity.

# session and login
path("", login_pages.login_page)  # apparently don't need login_redirect=IGNORE...
path("validate_login", login_pages.validate_login)  # and same here.
path("choose_study", admin_pages.choose_study)
path("logout", admin_pages.logout_admin, login_redirect=IGNORE)

# Researcher self administration
path("manage_credentials", admin_pages.manage_credentials, login_redirect=IGNORE)
path("researcher_change_my_password", admin_pages.researcher_change_my_password, login_redirect=IGNORE)
path("new_api_key", admin_pages.new_api_key)
path("disable_api_key", admin_pages.disable_api_key)
path("reset_mfa_self", admin_pages.reset_mfa_self, login_redirect=IGNORE)
path("test_mfa", admin_pages.test_mfa)

# The point of the thing
path("view_study/<int:study_id>", admin_pages.view_study, login_redirect=SAFE)

# Dashboard
path("dashboard/<int:study_id>", dashboard_api.dashboard_page, login_redirect=SAFE)
path(
    "dashboard/<int:study_id>/data_stream/<str:data_stream>",
    dashboard_api.get_data_for_dashboard_datastream_display,
    login_redirect=SAFE,
)
path(
    "dashboard/<int:study_id>/patient/<str:patient_id>",
    dashboard_api.dashboard_participant_page,
    login_redirect=SAFE,
)

# system admin pages
path("manage_researchers", system_admin_pages.manage_researchers, login_redirect=SAFE)
path(
    "edit_researcher/<int:researcher_pk>",
    system_admin_pages.edit_researcher_page,
    name="system_admin_pages.edit_researcher",
    login_redirect=SAFE,
)
path("elevate_researcher", system_admin_pages.elevate_researcher)
path("demote_researcher", system_admin_pages.demote_study_admin)
path("create_new_researcher", system_admin_pages.create_new_researcher)
path("manage_studies", system_admin_pages.manage_studies, login_redirect=SAFE)
path("edit_study/<int:study_id>", system_admin_pages.edit_study, login_redirect=SAFE)
path("reset_researcher_mfa/<int:researcher_id>", system_admin_pages.reset_researcher_mfa)

# study management
path("create_study", system_admin_pages.create_study)
path("toggle_study_forest_enabled/<int:study_id>", system_admin_pages.toggle_study_forest_enabled)
path("hide_study/<int:study_id>", system_admin_pages.hide_study)
path("edit_study_security/<int:study_id>", system_admin_pages.study_security_page, login_redirect=SAFE)
path("change_study_security_settings/<int:study_id>", system_admin_pages.change_study_security_settings)
path("device_settings/<int:study_id>", system_admin_pages.device_settings, login_redirect=SAFE)
path("update_end_date/<int:study_id>", system_admin_pages.update_end_date)
path("toggle_end_study/<int:study_id>", system_admin_pages.toggle_end_study)

# firebase credentials
path("manage_firebase_credentials", system_admin_pages.manage_firebase_credentials, login_redirect=SAFE)
path("upload_backend_firebase_cert", system_admin_pages.upload_backend_firebase_cert)
path("upload_android_firebase_cert", system_admin_pages.upload_android_firebase_cert)
path("upload_ios_firebase_cert", system_admin_pages.upload_ios_firebase_cert)
path("delete_backend_firebase_cert", system_admin_pages.delete_backend_firebase_cert)
path("delete_android_firebase_cert", system_admin_pages.delete_android_firebase_cert)
path("delete_ios_firebase_cert", system_admin_pages.delete_ios_firebase_cert)

# data access web form
path("data_access_web_form", data_access_web_form.data_api_web_form_page, login_redirect=SAFE)

# admin api
path('set_study_timezone/<str:study_id>', admin_api.set_study_timezone)
path('add_researcher_to_study', admin_api.add_researcher_to_study)
path('remove_researcher_from_study', admin_api.remove_researcher_from_study)
path('delete_researcher/<str:researcher_id>', admin_api.delete_researcher)
path('set_researcher_password', admin_api.set_researcher_password)
path('rename_study/<str:study_id>', admin_api.rename_study)
path("toggle_easy_enrollment_study/<int:study_id>", admin_api.toggle_easy_enrollment_study)

# app download
path("download", admin_api.download_current)
path("download_debug", admin_api.download_current_debug)
path("download_beta", admin_api.download_beta)
path("download_beta_debug", admin_api.download_beta_debug)
path("download_beta_release", admin_api.download_beta_release)
path("privacy_policy", admin_api.download_privacy_policy)

# study api
path('study/<str:study_id>/get_participants_api', study_api.study_participants_api)
path('study/<str:study_id>/download_participants_csv', study_api.download_participants_csv)
# study actions
path('interventions/<str:study_id>', study_api.interventions_page, login_redirect=SAFE)
path('delete_intervention/<str:study_id>', study_api.delete_intervention)
path('edit_intervention/<str:study_id>', study_api.edit_intervention)
path('study_fields/<str:study_id>', study_api.study_fields, login_redirect=SAFE)
path('delete_field/<str:study_id>', study_api.delete_field)
path('edit_custom_field/<str:study_id>', study_api.edit_custom_field)
# study data
path('download_study_intervention_history/<str:study_id>', study_api.download_study_interventions)
path('download_study_survey_history/<str:study_id>', study_api.download_study_survey_history)

# participant pages
path(
    'view_study/<int:study_id>/participant/<str:patient_id>/notification_history',
    participant_pages.notification_history,
    login_redirect=SAFE
)
path(
    'view_study/<int:study_id>/participant/<str:patient_id>',
    participant_pages.participant_page,
    login_redirect=SAFE
)
# experiments pages for participants
if ENABLE_EXPERIMENTS or RUNNING_TESTS:
    path(
        'view_study/<int:study_id>/participant/<str:patient_id>/experiments',
        participant_pages.experiments_page,
        login_redirect=SAFE
    )
    path(
        'view_study/<int:study_id>/participant/<str:patient_id>/update_experiments',
        participant_pages.update_experiments,
    )

# copy study api
path('export_study_settings_file/<str:study_id>', copy_study_api.export_study_settings_file)
path('import_study_settings_file/<str:study_id>', copy_study_api.import_study_settings_file)

# survey_api
path('create_survey/<str:study_id>/<str:survey_type>', survey_api.create_survey)
path('delete_survey/<str:study_id>/<str:survey_id>', survey_api.delete_survey)
path('update_survey/<str:study_id>/<str:survey_id>', survey_api.update_survey)
path('rename_survey/<str:study_id>/<str:survey_id>', survey_api.rename_survey)

# survey designer
path(
    'edit_survey/<str:study_id>/<str:survey_id>',
    survey_designer.render_edit_survey,
    login_redirect=SAFE,
)

# participant administration
path('reset_participant_password', participant_administration.reset_participant_password)
path('toggle_easy_enrollment', participant_administration.toggle_easy_enrollment)
path('clear_device_id', participant_administration.clear_device_id)
path('retire_participant', participant_administration.retire_participant)
path('create_new_participant', participant_administration.create_new_participant)
path('create_many_patients/<str:study_id>', participant_administration.create_many_patients)
path('delete_participant', participant_administration.delete_participant)

# push notification api
path('set_fcm_token', push_notifications_api.set_fcm_token)
path('test_notification', push_notifications_api.developer_send_test_notification)
path('send_survey_notification', push_notifications_api.developer_send_survey_notification)
path(
    '<int:study_id>/send_survey_notification/<str:patient_id>',
    push_notifications_api.resend_push_notification
)

# data access api and other researcher apis
path("get-data/v1", data_access_api.get_data)
path("get-studies/v1", other_data_apis.get_studies)
path("get-users/v1", other_data_apis.get_participant_ids_in_study)  # deprecated June 2024
path("get-participant-ids/v1", other_data_apis.get_participant_ids_in_study)
path("get-participant-data-info/v1", other_data_apis.get_participant_data_info)
path("get-interventions/v1", other_data_apis.download_study_interventions)
path("get-survey-history/v1", other_data_apis.download_study_survey_history)
path("get-participant-upload-history/v1", other_data_apis.get_participant_upload_history)
path("get-participant-heartbeat-history/v1", other_data_apis.get_participant_heartbeat_history)
path("get-participant-version-history/v1", other_data_apis.get_participant_version_history)
path("get-participant-table-data/v1", other_data_apis.get_participant_table_data)
path("get-summary-statistics/v1", other_data_apis.get_summary_statistics)
path("get-participant-device-status-history/v1", other_data_apis.get_participant_device_status_report_history)

# tableau
path(
    "api/v0/studies/<str:study_object_id>/summary-statistics/daily",
    tableau_api.get_tableau_daily
)
path(
    'api/v0/studies/<str:study_object_id>/summary-statistics/daily/wdc',
    tableau_api.web_data_connector
)

# forest pages
path('studies/<str:study_id>/forest/tasks/create', forest_pages.create_tasks)
path('studies/<str:study_id>/forest/tasks/copy', forest_pages.copy_forest_task)
path('studies/<str:study_id>/forest/progress', forest_pages.forest_tasks_progress, login_redirect=SAFE)
path("studies/<str:study_id>/forest/tasks/<str:forest_task_external_id>/cancel", forest_pages.cancel_task)
path('studies/<str:study_id>/forest/tasks', forest_pages.task_log, login_redirect=SAFE)
path('studies/<str:study_id>/forest/tasks/download', forest_pages.download_task_log)
path('studies/<str:study_id>/download_summary_statistics_csv/', forest_pages.download_summary_statistics_csv)
path('studies/<str:study_id>/download_participant_tree_data/<str:forest_task_external_id>', forest_pages.download_participant_tree_data)
path(
    "studies/<str:study_id>/forest/tasks/<str:forest_task_external_id>/download_output",
    forest_pages.download_output_data
)
path(
    "studies/<str:study_id>/forest/tasks/<str:forest_task_external_id>/download",
    forest_pages.download_task_data
)

## Endpoints related to the Apps

# Mobile api (includes ios targets, which require custom names)
path('upload', mobile_api.upload)
path('upload/ios', mobile_api.upload, name="mobile_api.upload_ios")
path('register_user', mobile_api.register_user)
path('register_user/ios', mobile_api.register_user, name="mobile_api.register_user_ios")
path('set_password', mobile_api.set_password)
path('set_password/ios', mobile_api.set_password, name="mobile_api.set_password_ios")
path('download_surveys', mobile_api.get_latest_surveys)
path('download_surveys/ios', mobile_api.get_latest_surveys, name="mobile_api.get_latest_surveys_ios")
path('get_latest_device_settings', mobile_api.get_latest_device_settings)
path('get_latest_device_settings/ios', mobile_api.get_latest_device_settings, name="mobile_api.get_latest_device_settings_ios")
path('mobile-heartbeat', mobile_api.mobile_heartbeat)
path('mobile-heartbeat/ios', mobile_api.mobile_heartbeat, name="mobile_api.mobile_heartbeat_ios")

# mobile pages
path('graph', mobile_pages.fetch_graph)



# add the static resource url patterns
urlpatterns.extend(static(settings.STATIC_URL, document_root=settings.STATIC_ROOT))
