from typing import Dict

from django.conf import settings
from django.conf.urls.static import static
from django.core.exceptions import ImproperlyConfigured
from django.urls import path as simplepath

from config.settings import ENABLE_EXPERIMENTS
from constants.common_constants import RUNNING_TESTS
from constants.url_constants import (IGNORE, LOGIN_REDIRECT_IGNORE, LOGIN_REDIRECT_SAFE, SAFE,
    urlpatterns)
from endpoints import (data_api_endpoints, data_page_endpoints, forest_endpoints, login_endpoints,
    manage_researcher_endpoints, manage_study_endpoints, misc_download_endpoints, mobile_endpoints,
    participant_endpoints, raw_data_api, study_endpoints, survey_endpoints, system_admin_endpoints)


def path(
    route: str, view: callable, name: str = None, kwargs: Dict = None, login_redirect: str = None
):
    """ Helper function, for creating url endpoints, automates our common logic across all urls.
    
    We want to automatically append a "/" to all urlpatterns.
    
    If no name is provided we insert the module and function name as the name, e.g.
    "module_name.function_name" like "login_endpoints.login_page"
    
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
path("", login_endpoints.login_page)  # apparently don't need login_redirect=IGNORE...
path("validate_login", login_endpoints.validate_login)  # and same here.
path("choose_study", study_endpoints.choose_study_page)
path("logout", login_endpoints.logout_page, login_redirect=IGNORE)

# Researcher self administration
path("manage_credentials", manage_researcher_endpoints.self_manage_credentials_page, login_redirect=IGNORE)
path("self_change_password", manage_researcher_endpoints.self_change_password, login_redirect=IGNORE)
path("generate_api_key", manage_researcher_endpoints.self_generate_api_key)
path("disable_api_key", manage_researcher_endpoints.self_disable_api_key)
path("self_reset_mfa", manage_researcher_endpoints.self_reset_mfa, login_redirect=IGNORE)
path("test_mfa", manage_researcher_endpoints.self_test_mfa)

# The point of the thing
path("view_study/<int:study_id>", study_endpoints.view_study_page, login_redirect=SAFE)

# Dashboard
path("dashboard/<int:study_id>", data_page_endpoints.dashboard_page, login_redirect=SAFE)
path(
    "dashboard/<int:study_id>/data_stream/<str:data_stream>",
    data_page_endpoints.get_data_for_dashboard_datastream_display,
    login_redirect=SAFE,
)
path(
    "dashboard/<int:study_id>/patient/<str:patient_id>",
    data_page_endpoints.dashboard_participant_page,
    login_redirect=SAFE,
)

# system admin pages
path("manage_researchers", manage_researcher_endpoints.administrator_manage_researchers_page, login_redirect=SAFE)
path(
    "edit_researcher/<int:researcher_pk>",
    manage_researcher_endpoints.administrator_edit_researcher_page,
    name="manage_researcher_endpoints.administrator_edit_researcher_page",
    login_redirect=SAFE,
)
path("elevate_researcher", manage_researcher_endpoints.administrator_elevate_researcher_to_study_admin)
path("demote_researcher", manage_researcher_endpoints.administrator_demote_study_administrator_to_researcher)
path("create_new_researcher", manage_researcher_endpoints.administrator_create_new_researcher)
path("manage_studies", study_endpoints.manage_studies_page, login_redirect=SAFE)
path("edit_study/<int:study_id>", study_endpoints.edit_study, login_redirect=SAFE)
path("reset_researcher_mfa/<int:researcher_id>", manage_researcher_endpoints.administrator_reset_researcher_mfa)

# study management
path("create_study", study_endpoints.create_study)
path("toggle_study_forest_enabled/<int:study_id>", study_endpoints.toggle_study_forest_enabled)
path("hide_study/<int:study_id>", study_endpoints.hide_study)
path("edit_study_security/<int:study_id>", study_endpoints.study_security_page, login_redirect=SAFE)
path("change_study_security_settings/<int:study_id>", study_endpoints.change_study_security_settings)
path("device_settings/<int:study_id>", study_endpoints.device_settings, login_redirect=SAFE)
path("update_end_date/<int:study_id>", study_endpoints.update_end_date)
path("toggle_end_study/<int:study_id>", study_endpoints.toggle_end_study)

# firebase credentials
path("manage_firebase_credentials", system_admin_endpoints.manage_firebase_credentials, login_redirect=SAFE)
path("upload_backend_firebase_cert", system_admin_endpoints.upload_backend_firebase_cert)
path("upload_android_firebase_cert", system_admin_endpoints.upload_android_firebase_cert)
path("upload_ios_firebase_cert", system_admin_endpoints.upload_ios_firebase_cert)
path("delete_backend_firebase_cert", system_admin_endpoints.delete_backend_firebase_cert)
path("delete_android_firebase_cert", system_admin_endpoints.delete_android_firebase_cert)
path("delete_ios_firebase_cert", system_admin_endpoints.delete_ios_firebase_cert)

# data access web form
path("data_access_web_form", data_page_endpoints.data_api_web_form_page, login_redirect=SAFE)

# admin api
path('set_study_timezone/<str:study_id>', manage_study_endpoints.set_study_timezone)
path('add_researcher_to_study', manage_researcher_endpoints.administrator_add_researcher_to_study)
path('remove_researcher_from_study', manage_researcher_endpoints.administrator_remove_researcher_from_study)
path('delete_researcher/<str:researcher_id>', manage_researcher_endpoints.administrator_delete_researcher)
path('set_researcher_password', manage_researcher_endpoints.administrator_set_researcher_password)
path('rename_study/<str:study_id>', manage_study_endpoints.rename_study)
path("toggle_easy_enrollment_study/<int:study_id>", manage_study_endpoints.toggle_easy_enrollment_study)

# app download
path("download", misc_download_endpoints.download_current)
path("download_debug", misc_download_endpoints.download_current_debug)
path("download_beta", misc_download_endpoints.download_beta)
path("download_beta_debug", misc_download_endpoints.download_beta_debug)
path("download_beta_release", misc_download_endpoints.download_beta_release)
path("privacy_policy", misc_download_endpoints.download_privacy_policy)

# study api
path('study/<str:study_id>/get_participants_api', manage_study_endpoints.study_participants_api)
path('study/<str:study_id>/download_participants_csv', manage_study_endpoints.download_participants_csv)
# study actions
path('interventions/<str:study_id>', manage_study_endpoints.interventions_page, login_redirect=SAFE)
path('delete_intervention/<str:study_id>', manage_study_endpoints.delete_intervention)
path('edit_intervention/<str:study_id>', manage_study_endpoints.edit_intervention)
path('study_fields/<str:study_id>', manage_study_endpoints.study_fields, login_redirect=SAFE)
path('delete_field/<str:study_id>', manage_study_endpoints.delete_field)
path('edit_custom_field/<str:study_id>', manage_study_endpoints.edit_custom_field)
# study data
path('download_study_intervention_history/<str:study_id>', manage_study_endpoints.download_study_interventions)
path('download_study_survey_history/<str:study_id>', manage_study_endpoints.download_study_survey_history)

# participant pages
path(
    'view_study/<int:study_id>/participant/<str:patient_id>/notification_history',
    participant_endpoints.notification_history,
    login_redirect=SAFE
)
path(
    'view_study/<int:study_id>/participant/<str:patient_id>',
    participant_endpoints.participant_page,
    login_redirect=SAFE
)
# experiments pages for participants
if ENABLE_EXPERIMENTS or RUNNING_TESTS:
    path(
        'view_study/<int:study_id>/participant/<str:patient_id>/experiments',
        participant_endpoints.experiments_page,
        login_redirect=SAFE
    )
    path(
        'view_study/<int:study_id>/participant/<str:patient_id>/update_experiments',
        participant_endpoints.update_experiments,
    )

# copy study api
path('export_study_settings_file/<str:study_id>', study_endpoints.export_study_settings_file)
path('import_study_settings_file/<str:study_id>', study_endpoints.import_study_settings_file)

# survey_api
path('create_survey/<str:study_id>/<str:survey_type>', survey_endpoints.create_survey)
path('delete_survey/<str:study_id>/<str:survey_id>', survey_endpoints.delete_survey)
path('update_survey/<str:study_id>/<str:survey_id>', survey_endpoints.update_survey)
path('rename_survey/<str:study_id>/<str:survey_id>', survey_endpoints.rename_survey)

# survey designer
path(
    'edit_survey/<str:study_id>/<str:survey_id>',
    survey_endpoints.render_edit_survey,
    login_redirect=SAFE,
)

# participant administration
path('reset_participant_password', participant_endpoints.reset_participant_password)
path('toggle_easy_enrollment', participant_endpoints.toggle_easy_enrollment)
path('clear_device_id', participant_endpoints.clear_device_id)
path('retire_participant', participant_endpoints.retire_participant)
path('create_new_participant', participant_endpoints.create_new_participant)
path('create_many_patients/<str:study_id>', participant_endpoints.create_many_patients)
path('delete_participant', participant_endpoints.delete_participant)

# push notification api
path('set_fcm_token', mobile_endpoints.set_fcm_token)
path('test_notification', mobile_endpoints.developer_send_test_notification)
path('send_survey_notification', mobile_endpoints.developer_send_survey_notification)
path(
    '<int:study_id>/send_survey_notification/<str:patient_id>',
    participant_endpoints.resend_push_notification
)

# data access api and other researcher apis
path("get-data/v1", raw_data_api.get_data)
path("get-studies/v1", data_api_endpoints.get_studies)
path("get-users/v1", data_api_endpoints.get_participant_ids_in_study)  # deprecated June 2024
path("get-participant-ids/v1", data_api_endpoints.get_participant_ids_in_study)
path("get-participant-data-quantities/v1", data_api_endpoints.get_participant_data_quantities)
path("get-interventions/v1", data_api_endpoints.download_study_interventions)
path("get-survey-history/v1", data_api_endpoints.download_study_survey_history)
path("get-participant-upload-history/v1", data_api_endpoints.get_participant_upload_history)
path("get-participant-heartbeat-history/v1", data_api_endpoints.get_participant_heartbeat_history)
path("get-participant-version-history/v1", data_api_endpoints.get_participant_version_history)
path("get-participant-table-data/v1", data_api_endpoints.get_participant_table_data)
path("get-summary-statistics/v1", data_api_endpoints.get_summary_statistics)
path("get-participant-device-status-history/v1", data_api_endpoints.get_participant_device_status_report_history)
path("get-participant-notification-history/v1", data_api_endpoints.get_participant_notification_history)

# tableau
path(
    "api/v0/studies/<str:study_object_id>/summary-statistics/daily",
    data_api_endpoints.get_tableau_daily
)
path(
    'api/v0/studies/<str:study_object_id>/summary-statistics/daily/wdc',
    data_api_endpoints.web_data_connector
)

# forest pages
path('studies/<str:study_id>/forest/tasks/create', forest_endpoints.create_tasks)
path('studies/<str:study_id>/forest/tasks/copy', forest_endpoints.copy_forest_task)
path('studies/<str:study_id>/forest/progress', forest_endpoints.forest_tasks_progress, login_redirect=SAFE)
path("studies/<str:study_id>/forest/tasks/<str:forest_task_external_id>/cancel", forest_endpoints.cancel_task)
path('studies/<str:study_id>/forest/tasks', forest_endpoints.task_log, login_redirect=SAFE)
path('studies/<str:study_id>/forest/tasks/download', forest_endpoints.download_task_log)
path('studies/<str:study_id>/download_summary_statistics_csv/', forest_endpoints.download_summary_statistics_csv)
path('studies/<str:study_id>/download_participant_tree_data/<str:forest_task_external_id>', forest_endpoints.download_participant_tree_data)
path(
    "studies/<str:study_id>/forest/tasks/<str:forest_task_external_id>/download_output",
    forest_endpoints.download_output_data
)
path(
    "studies/<str:study_id>/forest/tasks/<str:forest_task_external_id>/download",
    forest_endpoints.download_task_data
)

## Endpoints related to the Apps

# Mobile api (includes ios targets, which require custom names)
path('upload', mobile_endpoints.upload)
path('upload/ios', mobile_endpoints.upload, name="mobile_endpoints.upload_ios")
path('register_user', mobile_endpoints.register_user)
path('register_user/ios', mobile_endpoints.register_user, name="mobile_endpoints.register_user_ios")
path('set_password', mobile_endpoints.set_password)
path('set_password/ios', mobile_endpoints.set_password, name="mobile_endpoints.set_password_ios")
path('download_surveys', mobile_endpoints.get_latest_surveys)
path('download_surveys/ios', mobile_endpoints.get_latest_surveys, name="mobile_endpoints.get_latest_surveys_ios")
path('get_latest_device_settings', mobile_endpoints.get_latest_device_settings)
path('get_latest_device_settings/ios', mobile_endpoints.get_latest_device_settings, name="mobile_endpoints.get_latest_device_settings_ios")
path('mobile-heartbeat', mobile_endpoints.mobile_heartbeat)
path('mobile-heartbeat/ios', mobile_endpoints.mobile_heartbeat, name="mobile_endpoints.mobile_heartbeat_ios")

# mobile pages
path('graph', mobile_endpoints.fetch_graph)



# add the static resource url patterns
urlpatterns.extend(static(settings.STATIC_URL, document_root=settings.STATIC_ROOT))
