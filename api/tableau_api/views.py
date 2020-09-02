import json

from django import forms
from django.forms import ValidationError
from flask import request
from rest_framework import serializers
from rest_framework.renderers import JSONRenderer

from api.tableau_api.base import TableauApiView
from api.tableau_api.constants import SERIALIZABLE_FIELD_NAMES, VALID_QUERY_PARAMETERS
from database.tableau_api_models import SummaryStatisticDaily
from flask_cors import cross_origin


class SummaryStatisticDailySerializer(serializers.ModelSerializer):
    class Meta:
        model = SummaryStatisticDaily
        fields = SERIALIZABLE_FIELD_NAMES

    participant_id = serializers.SlugRelatedField(
        slug_field="patient_id", source="participant", read_only=True
    )
    study_id = serializers.SlugRelatedField(slug_field="object_id", source="study", read_only=True)

    def __init__(self, *args, fields=None, **kwargs):
        """ dynamically modify the subset of fields on instantiation """
        super().__init__(*args, **kwargs)

        if fields is not None:
            serialized_fields = set(fields)
            all_fields = set(self.fields)
            for field_name in all_fields - serialized_fields:
                self.fields.pop(field_name)


class SummaryStatisticDailyStudyView(TableauApiView):
    """
    API endpoint for retrieving SummaryStatisticsDaily objects for a study.
    """

    path = "/api/v0/studies/<string:study_id>/summary-statistics/daily"

    @cross_origin()
    def get(self, study_id):
        form = ApiQueryForm(data=request.values)
        if not form.is_valid():
            return self._render_errors(form.errors.get_json_data())
        query = form.cleaned_data
        fields = query.pop("fields", SERIALIZABLE_FIELD_NAMES)
        queryset = self._query_database(study_id=study_id, **query)
        serializer = SummaryStatisticDailySerializer(queryset, many=True, fields=fields)
        return JSONRenderer().render(serializer.data)

    @staticmethod
    def _query_database(
        study_id,
        end_date=None,
        start_date=None,
        limit=None,
        order_by="date",
        order_direction="descending",
        participant_ids=None,
    ):
        """
        Args:
            study_id (str): study in which to find data
            end_date (optional[date]): last date to include in search
            start_date (optional[date]): first date to include in search
            limit (optional[int]): maximum number of data points to return
            order_by (str): parameter to sort output by. Must be one in the list of fields to return
            order_direction (str): order to sort in, either "ascending" or "descending"
            participant_ids (optional[list[str]]): a list of participants to limit the search to

        Returns (queryset[SummaryStatisticsDaily]): the SummaryStatisticsDaily objects specified by the parameters
        """
        if order_direction == "descending":
            order_by = "-" + order_by
        queryset = SummaryStatisticDaily.objects.filter(study__object_id=study_id)
        if participant_ids:
            queryset = queryset.filter(participant__patient_id__in=participant_ids)
        if end_date:
            queryset = queryset.filter(date__lte=end_date)
        if start_date:
            queryset = queryset.filter(date__gte=start_date)
        queryset = queryset.order_by(order_by)
        if limit:
            queryset = queryset[:limit]
        return queryset

    @staticmethod
    def _render_errors(errors):
        messages = []
        for field, field_errs in errors.items():
            messages.extend([err["message"] for err in field_errs])
        return json.dumps({"errors": messages})


class CommaSeparatedListFieldMixin:
    """ A mixin for use with django form fields. This mixin changes the field to accept a comma separated list of
        inputs that are individually cleaned and validated. Takes one optional parameter, list_validators, which is
        a list of validators to be applied to the final list of values (the validator parameter still expects a single
        value as input, and is applied to each value individually) """

    def __init__(self, list_validators=None, *args, **kwargs):
        self.list_validators = list_validators if list_validators is not None else []
        super().__init__(*args, **kwargs)

    def clean(self, value):
        if value:
            if not isinstance(value, str):
                raise TypeError(
                    "a non string argument was supplied to a CommaSeparatedListField"
                )
            value_list = value.split(",")
        else:
            value_list = []
        errors = []
        cleaned_values = []
        for v in value_list:
            try:
                cleaned_values.append(super(CommaSeparatedListFieldMixin, self).clean(v.strip()))
            except ValidationError as err:
                errors.append(err)
        if errors:
            raise ValidationError(errors)
        for validator in self.list_validators:
            validator(cleaned_values)
        return cleaned_values


class CommaSeparatedListCharField(CommaSeparatedListFieldMixin, forms.CharField):
    pass


class CommaSeparatedListChoiceField(CommaSeparatedListFieldMixin, forms.ChoiceField):
    pass


class ApiQueryForm(forms.Form):
    end_date = forms.DateField(
        required=False,
        error_messages={
            "invalid": "end date could not be interpreted as a date. Dates should be"
            "formatted as YYYY-MM-DD"
        },
    )

    start_date = forms.DateField(
        required=False,
        error_messages={
            "invalid": "start date could not be interpreted as a date. Dates should be"
            "formatted as YYYY-MM-DD"
        },
    )

    limit = forms.IntegerField(
        required=False,
        error_messages={"invalid": "limit value could not be interpreted as an integer value"},
    )

    order_by = forms.ChoiceField(
        choices=[(f, f) for f in SERIALIZABLE_FIELD_NAMES],
        required=False,
        error_messages={
            "invalid_choice": "%(value)s is not a field that can be used " "to sort the output"
        },
    )

    order_direction = forms.ChoiceField(
        choices=[("ascending", "ascending"), ("descending", "descending")],
        required=False,
        error_messages={
            "invalid_choice": "If provided, the order_direction parameter "
            "should contain either the value 'ascending' "
            "or 'descending'"
        },
    )

    participant_ids = CommaSeparatedListCharField(required=False)

    fields = CommaSeparatedListChoiceField(
        choices=[(f, f) for f in SERIALIZABLE_FIELD_NAMES],
        required=False,
        error_messages={"invalid_choice": "%(value)s is not a valid field"},
    )

    def clean(self):
        """ removes invalid query parameters and parameters not provided from the cleaned data """
        super().clean()
        return {
            k: v
            for (k, v) in self.cleaned_data.items()
            if k in VALID_QUERY_PARAMETERS and (v or v is False)
        }
