from rest_framework import serializers

from database.security_models import ApiKey


class ApiKeySerializer(serializers.ModelSerializer):
    class Meta:
        model = ApiKey
        fields = ['created_on', 'access_key_id', 'is_active', 'has_tableau_api_permissions', 'readable_name']
