import json

from flask import request
from django import forms
from django.forms import ValidationError
from rest_framework import serializers
from rest_framework.renderers import JSONRenderer

from api.tableau_api.base import TableauApiView
from database.tableau_api_models import SummaryStatisticDaily
from api.tableau_api.constants import field_names, valid_query_parameters


class SummaryStatisticDailySerializer(serializers.ModelSerializer):
    class Meta:
        model = SummaryStatisticDaily
        fields = field_names
    participant_id = serializers.SlugRelatedField(slug_field="patient_id", source='participant', read_only=True)
    study_id = serializers.SlugRelatedField(slug_field="object_id", source='study', read_only=True)

    #  dynamically modify the subset of fields on instantiation
    def __init__(self, *args, **kwargs):
        # Don't pass the 'fields' arg up to the superclass
        fields = kwargs.pop('fields', None)

        # Instantiate the superclass normally
        super().__init__(*args, **kwargs)

        if fields is not None:
            # Drop any fields that are not specified in the `fields` argument.
            allowed = set(fields)
            existing = set(self.fields)
            for field_name in existing - allowed:
                self.fields.pop(field_name)


class SummaryStatisticDailyStudyView(TableauApiView):
    """
    API endpoint for retrieving SummaryStatisticsDaily objects for a study.
    """
    path = '/api/v0/studies/<string:study_id>/summary-statistics/daily'

    def get(self, study_id):
        request.values = dict(request.values)
        form = ApiQueryForm(data=request.values)
        if not form.is_valid():
            return self._render_errors(form.errors.get_json_data())
        query = form.cleaned_data
        fields = query.pop("fields", field_names)
        queryset = self._query_database(study_id=study_id, **query)
        serializer = SummaryStatisticDailySerializer(queryset, many=True, fields=fields)
        return JSONRenderer().render(serializer.data)

    @staticmethod
    def _query_database(study_id, end_date=None, start_date=None, limit=None, ordered_by='date',
                        order_direction='descending', participant_ids=None):
        """
        study_id : string
        end_date/start_date : date object or None
        limit: int or None
        ordered_by : string drawn from the list of fields
        order_direction: string, either 'ascending' or 'descending'
        participant_ids : list of strings or None
        fields: unused but left for convenience as it is a query parameter
        returns a queryset
        """
        if order_direction == 'descending':
            ordered_by = '-' + ordered_by
        queryset = SummaryStatisticDaily.objects.filter(study__object_id=study_id).filter(deleted=False)
        if participant_ids:
            queryset = queryset.filter(participant__patient_id__in=participant_ids)
        if end_date:
            queryset = queryset.filter(date__lte=end_date)
        if start_date:
            queryset = queryset.filter(date__gte=start_date)
        queryset = queryset.order_by(ordered_by)
        if limit:
            queryset = queryset[:limit]  # seems to be the standard method in docs?
        return queryset

    @staticmethod
    def _render_errors(errors):
        messages = []
        for field, field_errs in errors.items():
            #  messages.extend([{"%s" % (field): err["message"]} for err in field_errs])
            #  messages.extend(["in field '" + field + "': " + err["message"] for err in field_errs])
            messages.extend([err["message"] for err in field_errs])
        return json.dumps({"errors": messages})


class CommaSeparatedListField(forms.CharField):
    def clean(self, value):
        value = super().clean(value)
        value = value.split(",")
        if value == [""]:
            return None
        return value


class MultiErrorMultipleChoiceField(forms.MultipleChoiceField):
    def validate(self, value):
        errs = []
        for val in value:
            try:
                super().validate([val])
            except ValidationError as e:
                errs.append(e)
        if errs:
            raise ValidationError(errs, code='invalid_choice')


class ApiQueryForm(forms.Form):
    #  overrides the constructor to interpret a string for fields as a comma separated list of choices
    def __init__(self, *args, **kwargs):
        if "data" in kwargs and "fields" in kwargs["data"] and isinstance(kwargs["data"]["fields"], str):
            fields = kwargs["data"]["fields"].split(",")
            if fields == [""]:
                fields = None
            kwargs["data"]["fields"] = fields
        super().__init__(*args, **kwargs)

    end_date = forms.DateField(required=False,
                               error_messages={'invalid': 'end date could not be interpreted as a date. Dates should be'
                                                          'formatted as "YYYY-MM-DD" (without quotes)'})

    start_date = forms.DateField(required=False,
                                 error_messages={
                                     'invalid': 'start date could not be interpreted as a date. Dates should be'
                                                'formatted as "YYYY-MM-DD" (without quotes)'})

    limit = forms.IntegerField(required=False,
                               error_messages={'invalid': "limit value could not be interpreted as an integer value"})

    ordered_by = forms.ChoiceField(choices=[(f, f) for f in field_names],
                                   required=False,
                                   error_messages={'invalid_choice': "%(value)s is not a field that can be used "
                                                                     "to sort the output"})

    order_direction = forms.ChoiceField(choices=[('ascending', 'ascending'), ('descending', 'descending')],
                                        required=False,
                                        error_messages={'invalid_choice': "If provided, the order_direction parameter "
                                                                          "should contain either the value 'ascending' "
                                                                          "or 'descending'"})

    participant_ids = CommaSeparatedListField(required=False)

    fields = MultiErrorMultipleChoiceField(choices=[(f, f) for f in field_names],
                                           required=False,
                                           error_messages={'invalid_choice': '%(value)s is not a valid field'})

    def clean(self, *args, **kwargs):
        # remove falsy outputs from the cleaned data
        super().clean(*args, **kwargs)
        return {k: v for (k, v) in self.cleaned_data.items() if k in valid_query_parameters
                                                                and (v or v is False)}
