from django.conf import settings
from django.conf.urls.static import static
from django.urls import path as simplepath

from api import (admin_api, copy_study_api, dashboard_api, data_access_api, mobile_api,
    other_researcher_apis, participant_administration, push_notifications_api, study_api,
    survey_api, tableau_api)
from pages import (admin_pages, data_access_web_form, forest_pages, login_pages, mobile_pages,
    participant_pages, survey_designer, system_admin_pages)


def path(route: str, view: callable, name: str = None, kwargs=None):
    """ Helper function, for creating url paths and appending them to `urlpatterns`.
    This function mimics the parameters passable into the simplepath (actually `path`) class.
    
    If no name is provided we insert the module and function name as the name, e.g.
    "module_name.function_name" like "login_pages.login_page" """
    
    if name is None:
        name = view.__module__.rsplit(".", 1)[-1] + "." + view.__name__
    
    route_with_slash = route if route.endswith("/") else route + "/"
    route_without_slash = route if not route.endswith("/") else route.rstrip("/")
    urlpatterns.append(simplepath(route_with_slash, view, name=name, kwargs=kwargs))
    urlpatterns.append(simplepath(route_without_slash, view, name=name, kwargs=kwargs))


urlpatterns = []

# session and login
path("", login_pages.login_page)
path("validate_login", login_pages.validate_login)
path("choose_study", admin_pages.choose_study)
path("logout", admin_pages.logout_admin)

# Admin
path("view_study/<int:study_id>", admin_pages.view_study)
path("manage_credentials", admin_pages.manage_credentials)
path("reset_admin_password", admin_pages.reset_admin_password)
path("reset_download_api_credentials", admin_pages.reset_download_api_credentials)
path("new_api_key", admin_pages.new_tableau_api_key)
path("disable_tableau_api_key", admin_pages.disable_tableau_api_key)

# Dashboard
path("dashboard/<int:study_id>", dashboard_api.dashboard_page)
path(
    "dashboard/<int:study_id>/data_stream/<str:data_stream>",
    dashboard_api.get_data_for_dashboard_datastream_display
)
path(
    "dashboard/<int:study_id>/patient/<str:patient_id>",
    dashboard_api.dashboard_participant_page
)

# system admin pages
path("manage_researchers", system_admin_pages.manage_researchers)
path(
    "edit_researcher/<int:researcher_pk>",
    system_admin_pages.edit_researcher_page,
    name="system_admin_pages.edit_researcher"
)
path("elevate_researcher", system_admin_pages.elevate_researcher)
path("demote_researcher", system_admin_pages.demote_study_admin)
path("create_new_researcher", system_admin_pages.create_new_researcher)
path("manage_studies", system_admin_pages.manage_studies)
path("edit_study/<int:study_id>", system_admin_pages.edit_study)
path("create_study", system_admin_pages.create_study)
path("toggle_study_forest_enabled/<int:study_id>", system_admin_pages.toggle_study_forest_enabled)
path("delete_study/<int:study_id>", system_admin_pages.delete_study)
path("device_settings/<int:study_id>", system_admin_pages.device_settings)
path("manage_firebase_credentials", system_admin_pages.manage_firebase_credentials)
path("upload_backend_firebase_cert", system_admin_pages.upload_backend_firebase_cert)
path("upload_android_firebase_cert", system_admin_pages.upload_android_firebase_cert)
path("upload_ios_firebase_cert", system_admin_pages.upload_ios_firebase_cert)
path("delete_backend_firebase_cert", system_admin_pages.delete_backend_firebase_cert)
path("delete_android_firebase_cert", system_admin_pages.delete_android_firebase_cert)
path("delete_ios_firebase_cert", system_admin_pages.delete_ios_firebase_cert)

# data access web form
path("data_access_web_form", data_access_web_form.data_api_web_form_page)

# admin api
path('set_study_timezone/<str:study_id>', admin_api.set_study_timezone)
path('add_researcher_to_study', admin_api.add_researcher_to_study)
path('remove_researcher_from_study', admin_api.remove_researcher_from_study)
path('delete_researcher/<str:researcher_id>', admin_api.delete_researcher)
path('set_researcher_password', admin_api.set_researcher_password)
path('rename_study/<str:study_id>', admin_api.rename_study)

# app download
path("download", admin_api.download_current)
path("download_debug", admin_api.download_current_debug)
path("download_beta", admin_api.download_beta)
path("download_beta_debug", admin_api.download_beta_debug)
path("download_beta_release", admin_api.download_beta_release)
path("privacy_policy", admin_api.download_privacy_policy)

# study api
path('study/<str:study_id>/get_participants_api', study_api.study_participants_api)
path('interventions/<str:study_id>', study_api.interventions_page)
path('delete_intervention/<str:study_id>', study_api.delete_intervention)
path('edit_intervention/<str:study_id>', study_api.edit_intervention)
path('download_study_intervention_history/<str:study_id>', study_api.download_study_interventions)
path('download_study_survey_history/<str:study_id>', study_api.download_study_survey_history)
path('study_fields/<str:study_id>', study_api.study_fields)
path('delete_field/<str:study_id>', study_api.delete_field)
path('edit_custom_field/<str:study_id>', study_api.edit_custom_field)

# participant pages
path(
    'view_study/<int:study_id>/participant/<str:patient_id>/notification_history',
    participant_pages.notification_history
)
path('view_study/<int:study_id>/participant/<str:patient_id>', participant_pages.participant_page)
path('view_study/<int:study_id>/participant/<str:patient_id>/message/schedule', participant_pages.schedule_message)
path('view_study/<int:study_id>/participant/<str:patient_id>/message/<str:participant_message_uuid>/cancel', participant_pages.cancel_message)

# copy study api
path('export_study_settings_file/<str:study_id>', copy_study_api.export_study_settings_file)
path('import_study_settings_file/<str:study_id>', copy_study_api.import_study_settings_file)

# survey_api
path('create_survey/<str:study_id>/<str:survey_type>', survey_api.create_survey)
path('delete_survey/<str:study_id>/<str:survey_id>', survey_api.delete_survey)
path('update_survey/<str:study_id>/<str:survey_id>', survey_api.update_survey)
path('rename_survey/<str:study_id>/<str:survey_id>', survey_api.rename_survey)

# survey designer
path('edit_survey/<str:study_id>/<str:survey_id>', survey_designer.render_edit_survey)

# participant administration
path('reset_participant_password', participant_administration.reset_participant_password)
path('reset_device', participant_administration.reset_device)
path('unregister_participant', participant_administration.unregister_participant)
path('create_new_participant', participant_administration.create_new_participant)
path('create_many_patients/<str:study_id>', participant_administration.create_many_patients)

# push notification api
path('set_fcm_token', push_notifications_api.set_fcm_token)
path('test_notification', push_notifications_api.developer_send_test_notification)
path('send_survey_notification', push_notifications_api.developer_send_survey_notification)
path(
    '<int:study_id>/send_survey_notification/<str:patient_id>',
    push_notifications_api.resend_push_notification
)


# other researcher apis
path("get-studies/v1", other_researcher_apis.get_studies)
path("get-users/v1", other_researcher_apis.get_users_in_study)
path("get-interventions/v1", other_researcher_apis.download_study_interventions)

# data_access_api
path("get-data/v1", data_access_api.get_data)

# Mobile api (includes ios targets, which require custom names)
path('upload', mobile_api.upload)
path('upload/ios', mobile_api.upload, name="mobile_api.upload_ios")
path('register_user', mobile_api.register_user)
path('register_user/ios', mobile_api.register_user, name="mobile_api.register_user_ios")
path('set_password', mobile_api.set_password)
path('set_password/ios', mobile_api.set_password, name="mobile_api.set_password_ios")
path('download_surveys', mobile_api.get_latest_surveys)
path('download_surveys/ios', mobile_api.get_latest_surveys, name="mobile_api.get_latest_surveys_ios")

# mobile pages
path('graph', mobile_pages.fetch_graph)

# forest pages
path('studies/<str:study_id>/forest/progress', forest_pages.analysis_progress)
path('studies/<str:study_id>/forest/tasks/create', forest_pages.create_tasks)
path('studies/<str:study_id>/forest/tasks', forest_pages.task_log)
path('forest/tasks/download', forest_pages.download_task_log)
path("studies/<str:study_id>/forest/tasks/<str:forest_task_external_id>/cancel", forest_pages.cancel_task)
path(
    "studies/<str:study_id>/forest/tasks/<str:forest_task_external_id>/download",
    forest_pages.download_task_data
)

# tableau
path(
    "api/v0/studies/<str:study_object_id>/summary-statistics/daily",
    tableau_api.get_tableau_daily
)
path(
    'api/v0/studies/<str:study_object_id>/summary-statistics/daily/wdc',
    tableau_api.web_data_connector
)

# add the static resource url patterns
urlpatterns.extend(static(settings.STATIC_URL, document_root=settings.STATIC_ROOT))
