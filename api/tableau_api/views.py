from api.tableau_api.base import TableauApiView
from database.tableau_api_models import SummaryStatisticDaily
from django.core import serializers
from flask import request
from datetime import date, datetime
from dateutil.parser import parse
from django.core.exceptions import FieldError
from django import forms
from database.study_models import Study
from database.user_models import Participant
from django.forms.models import model_to_dict
from django.core.serializers.json import Serializer as jsonSerializer
from django.forms import ValidationError
import json


class CleanSerializer(jsonSerializer):
    #  https://stackoverflow.com/questions/5453237/override-django-object-serializer-to-get-rid-of-specified-model
    def get_dump_object(self, obj):
        return self._current


class DatabaseQueryFailed(Exception):
    status_code = 400


class InvalidInput(Exception):
    status_code = 400


class SummaryStatisticDailyStudyView(TableauApiView):
    """
    API endpoint for retrieving SummaryStatisticsDaily objects for a study.
    """
    path = '/api/v0/studies/<string:study_id>/summary-statistics/daily'

    def get(self, study_id):
        request.values = dict(request.values)
        request.values['study_id'] = study_id
        errors, query = self._validate_query(**request.values)
        if errors:
            return self._process_errs(errors)
        print(query)
        queryset = self._query_database(**query)
        json_serializer = CleanSerializer()
        json_serializer.serialize(queryset, fields=query['fields'])
        data = json_serializer.getvalue()  # possibly useful to optimize to write to a file directly/stream?
        return data

    def _query_database(self, study_id, end_date=None, start_date=None, limit=None, ordered_by='date',
                        order_direction='descending', participant_ids=None, fields=None):
        """
        study_id : int
        end_date/start_date : date object
        limit: int
        ordered_by : string drawn from the list of fields
        order_direction: string, either ascending/descending
        participant_ids : list of ints
        fields: any
        """
        if order_direction.lower() == 'descending':
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
            queryset = queryset[:int(limit)]  # consider edge cases + queryset limit
        return queryset

    @staticmethod
    def _process_errs(errors):
        #  possibly awkward behavior for nested error messages (untested)
        #  print(errors)
        #  messages = [{k: v[0]['message']} for k, v in errors.items()]
        messages = []
        for field, field_errs in errors.items():
            messages.extend([{"%s(%i)" % (field, num+1): err["message"]} for num, err in enumerate(field_errs)])
        return json.dumps({"errors": messages})

    @staticmethod
    def _validate_query(**kwargs):
        fields = kwargs.get('fields', '')
        fields = fields.split(',')
        if fields == ['']:
            fields = field_names

        query = {'study_id': kwargs.get('study_id'),
                 'end_date': kwargs.get('end_date', None),
                 'start_date': kwargs.get('start_date', None),
                 'limit': kwargs.get('limit', None),
                 'ordered_by': kwargs.get('ordered_by', 'date'),
                 'order_direction': kwargs.get('order_direction', 'descending'),
                 'participant_ids': kwargs.get('participant_ids', ''),
                 'fields': fields}

        form = ApiQueryForm(data=query)
        if not form.is_valid():
            return form.errors.as_json_data(), None
        return None, form.cleaned_data


class CleanSerializer(jsonSerializer):
    #  https://stackoverflow.com/questions/5453237/override-django-object-serializer-to-get-rid-of-specified-model
    def get_dump_object(self, obj):
        return self._current


field_names = ["participant",
               "study",
               "date",
               "distance_diameter",
               "distance_from_home",
               "distance_travelled",
               "flight_distance_average",
               "flight_distance_standard_deviation",
               "flight_duration_average",
               "flight_duration_standard_deviation",
               "gps_data_missing_duration",
               "home_duration",
               "physical_circadian_rhythm",
               "physical_circadian_rhythm_stratified",
               "radius_of_gyration",
               "significant_location_count",
               "significant_location_entropy",
               "stationary_fraction",
               "text_incoming_count",
               "text_incoming_degree",
               "text_incoming_length",
               "text_incoming_responsiveness",
               "text_outgoing_count",
               "text_outgoing_degree",
               "text_outgoing_length",
               "text_reciprocity",
               "call_incoming_count",
               "call_incoming_degree",
               "call_incoming_duration",
               "call_incoming_responsiveness",
               "call_outgoing_count",
               "call_outgoing_degree",
               "call_outgoing_duration",
               "acceleration_direction",
               "accelerometer_coverage_fraction",
               "accelerometer_signal_variability",
               "accelerometer_univariate_summaries",
               "device_proximity",
               "total_power_events",
               "total_screen_events",
               "total_unlock_events",
               "awake_onset_time",
               "sleep_duration",
               "sleep_onset_time"]

class CsvField(forms.CharField):
    def clean(self, value):
        value = super().clean(value)
        value = value.split(",")
        if value == [""]:
            return None
        return value


class ApiQueryForm(forms.Form):
    # study_id is cleaned to the object ID of the chosen Study
    study_id = forms.ModelChoiceField(queryset=Study.objects.all(),
                                      required=True)

    end_date = forms.DateField(required=False)

    start_date = forms.DateField(required=False)

    limit = forms.IntegerField(required=False)

    ordered_by = forms.ChoiceField(choices=[(f, f) for f in field_names],
                                   required=False,
                                   error_messages={'invalid_choice': "%(value)s is not a field that can be used "
                                                                     "to sort the output"})

    order_direction = forms.ChoiceField(choices=[('ascending', 'ascending'), ('descending', 'descending')],
                                        required=False)

    #  participant_ids is cleaned to a list of IDs of participants
    #  change to not raise an error or checking participant ids for validity
    participant_ids = CsvField(required=False)

    fields = forms.MultipleChoiceField(choices=[(f, f) for f in field_names],
                                       required=False)

    def clean_study_id(self):
        # cleans from instance of study to its ID
        data = self.cleaned_data['study_id']
        return data.object_id

    # def clean_participant_ids(self):
    #     # cleans from a queryset to a list of IDs or None
    #     data = self.cleaned_data['participant_ids']
    #     if not data or data == ['']:
    #         return None
    #     data = [str(d.patient_id) for d in data]
    #     return data
    #     # queryset -> list of strings or None

    def clean_fields(self):
        data = self.cleaned_data['fields']
        if not data or data == ['']:
            return None
        return data



