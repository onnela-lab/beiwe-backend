from django import forms
from django.core.exceptions import ObjectDoesNotExist
from flask import jsonify, request
from flask.views import MethodView

from api.tableau_api.constants import (APIKEY_NO_ACCESS_MESSAGE, HEADER_IS_REQUIRED,
                                       NO_STUDY_FOUND_MESSAGE, NO_STUDY_PROVIDED_MESSAGE,
                                       RESEARCHER_NOT_ALLOWED, RESOURCE_NOT_FOUND,
                                       STUDY_HAS_FOREST_DISABLED_MESSAGE, X_ACCESS_KEY_ID,
                                       X_ACCESS_KEY_SECRET, CREDENTIALS_NOT_VALID_ERROR_MESSAGE)
from database.security_models import ApiKey
from database.study_models import Study
from database.user_models import StudyRelation


class AuthenticationFailed(Exception): pass
class PermissionDenied(Exception): pass


class AuthenticationForm(forms.Form):
    """
    Form for fetching request headers
    """

    def __init__(self, *args, **kwargs):
        """
        Define authentication form fields since the keys contain illegal characters for
        variable names.
        """
        super().__init__(*args, **kwargs)
        self.fields[X_ACCESS_KEY_ID] = forms.CharField(
            error_messages={"required": HEADER_IS_REQUIRED}
        )
        self.fields[X_ACCESS_KEY_SECRET] = forms.CharField(
            error_messages={"required": HEADER_IS_REQUIRED}
        )


class TableauApiView(MethodView):
    """
    The base class for all Tableau API views that implements authentication and other functionality
    specific to this API.
    """

    def check_permissions(self, *args, study_object_id=None, **kwargs):
        """
        Authenticate API key and check permissions for access to a study/participant data.
        """
        form = AuthenticationForm(request.headers)
        if not form.is_valid():
            raise AuthenticationFailed(form.errors)
        try:
            api_key = ApiKey.objects.get(
                access_key_id=form.cleaned_data[X_ACCESS_KEY_ID], is_active=True,
            )
        except ApiKey.DoesNotExist:
            raise AuthenticationFailed(CREDENTIALS_NOT_VALID_ERROR_MESSAGE)

        if not api_key.proposed_secret_key_is_valid(form.cleaned_data[X_ACCESS_KEY_SECRET]):
            raise AuthenticationFailed(CREDENTIALS_NOT_VALID_ERROR_MESSAGE)

        # Authorization
        if not api_key.has_tableau_api_permissions:
            raise PermissionDenied(APIKEY_NO_ACCESS_MESSAGE)

        if study_object_id is None:
            raise PermissionDenied(NO_STUDY_PROVIDED_MESSAGE)
        if not Study.objects.filter(object_id=study_object_id).exists():
            raise PermissionDenied(NO_STUDY_FOUND_MESSAGE)

        if not Study.objects.get(object_id=study_object_id).forest_enabled:
            raise PermissionDenied(STUDY_HAS_FOREST_DISABLED_MESSAGE)

        if api_key.researcher.site_admin:
            return True

        try:
            StudyRelation.objects.filter(study__object_id=study_object_id).get(researcher=api_key.researcher)
        except ObjectDoesNotExist:
            raise PermissionDenied(RESEARCHER_NOT_ALLOWED)

        return True

    def dispatch_request(self, *args, **kwargs):
        """
        Override `super().dispatch_request` to return 404 if a method is not allowed.
        """
        try:
            self.check_permissions(*args, **kwargs)
        except AuthenticationFailed as error:
            response = jsonify({"errors": error.args})
            response.status_code = 400
            return response
        except PermissionDenied:
            # Prefer 404 over 403 to hide information about validity of these resource identifiers
            response = jsonify({"errors": RESOURCE_NOT_FOUND})
            response.status_code = 404
            return response
        return super().dispatch_request(*args, **kwargs)

    @classmethod
    def register_urls(cls, app):
        """
        Register this class' URLs with Flask
        """
        app.add_url_rule(cls.path, view_func=cls.as_view("summary_statistics_daily_study_view"))
