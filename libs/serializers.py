from flask import url_for
from rest_framework import serializers

from config.constants import DEV_TIME_FORMAT
from database.security_models import ApiKey
from database.tableau_api_models import ForestTask


class ApiKeySerializer(serializers.ModelSerializer):
    class Meta:
        model = ApiKey
        fields = [
            "access_key_id",
            "created_on",
            "has_tableau_api_permissions",
            "is_active",
            "readable_name",
        ]


class ForestTrackerSerializer(serializers.ModelSerializer):
    cancel_url = serializers.SerializerMethodField()
    created_on_display = serializers.SerializerMethodField()
    download_url = serializers.SerializerMethodField()
    forest_params = serializers.SerializerMethodField()
    forest_tree_display = serializers.SerializerMethodField()
    forest_param_name = serializers.SerializerMethodField()
    forest_param_notes = serializers.SerializerMethodField()
    patient_id = serializers.SerializerMethodField()
    
    class Meta:
        model = ForestTask
        fields = [
            "cancel_url",
            "created_on_display",
            "data_date_end",
            "data_date_start",
            "download_url",
            "forest_params",
            "forest_tree_display",
            "forest_param_name",
            "forest_param_notes",
            "patient_id",
            "process_download_end_time",
            "process_start_time",
            "process_end_time",
            "stacktrace",
            "status",
            "total_file_size",
        ]

    def get_cancel_url(self, instance):
        return url_for(
            "forest_pages.cancel_task",
            study_id=instance.participant.study_id,
            forest_tracker_external_id=instance.external_id,
        )
    
    def get_created_on_display(self, instance):
        return instance.created_on.strftime(DEV_TIME_FORMAT)
    
    def get_download_url(self, instance):
        return url_for(
            "forest_pages.download_task_data",
            study_id=instance.participant.study_id,
            forest_tracker_external_id=instance.external_id,
        )
    
    def get_forest_tree_display(self, instance):
        return instance.forest_tree.title()
    
    def get_forest_params(self, instance):
        return repr(instance.params_dict())
    
    def get_forest_param_name(self, instance):
        return instance.forest_param.name
    
    def get_forest_param_notes(self, instance):
        return instance.forest_param.notes
    
    def get_patient_id(self, instance):
        return instance.participant.patient_id
