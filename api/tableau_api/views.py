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
from django.core.serializers import json


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
        errors, query = _validate_and_coerce_query(**request.values)
        if errors:
            return errors
        queryset = self._query_database(**query)
        json_serializer = CleanSerializer()
        json_serializer.serialize(queryset, fields=query.get('fields', None))
        data = json_serializer.getvalue()
        return data

    @staticmethod
    def _query_database(study_id, end_date=None, start_date=None, limit=None, ordered_by='date',
                        order_direction='descending', participant_ids=None, fields=None):
        """
        study_id : int
        end_date/start_date : date object or None
        limit: int
        ordered_by : string drawn from the list of fields
        order_direction: string, either ascending/descending
        participant_ids : list of ints
        fields: any
        """
        if order_direction.lower() == 'descending':
            ordered_by = '-' + ordered_by
        queryset = SummaryStatisticDaily.objects.filter(study__object_id=study_id)
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


class CleanSerializer(json.Serializer):
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
# TODO, error messages


class ApiQueryForm(forms.Form):
    study_id = forms.ModelChoiceField(queryset=Study.objects.all(),
                                      required=True)
    # overwritten to clean to a string

    end_date = forms.DateField(required=False,
                               error_messages={'invalid': "end date could not be interpreted as a date"})

    start_date = forms.DateField(required=False,
                                 error_messages={'invalid': "start date could not be interpreted as a date"})

    limit = forms.IntegerField(required=False,
                               error_messages={'invalid': "limit could not be interpreted as an integer value"})

    ordered_by = forms.ChoiceField(choices=[(f, f) for f in field_names],
                                   required=False,
                                   error_messages={'invalid_choice': "%(value)s is not a field that can be used "
                                                                     "to sort the output"})

    order_direction = forms.ChoiceField(choices=[('ascending', 'ascending'), ('descending', 'descending')],
                                        required=False,
                                        error_messages={'invalid_choice': "If provided, the order_direction parameter "
                                                                          "should contain either the value 'ascending' "
                                                                          "or 'descending'"})

    participant_ids = forms.ModelMultipleChoiceField(queryset=Participant.objects.all(),
                                                     required=False,
                                                     error_messages={'invalid_choice': '%(value)s is not a valid '
                                                                                       'patient id'})

    fields = forms.MultipleChoiceField(choices=[(f, f) for f in field_names],
                                       required=False,
                                       error_messages={'invalid_choice': '%(value)s is not a valid field'})

    def clean_study_id(self):
        data = self.cleaned_data['study_id']
        return data.object_id

    def clean_participant_ids(self):
        # queryset -> list of strings or None
        data = self.cleaned_data['participant_ids']
        data = [str(d.patient_id) for d in data]
        if not data:
            return None
        return data

    def clean_fields(self):
        data = self.cleaned_data['fields']
        if not data:
            return None
        return data

    def __init__(self, *args, **kwargs):
        super.__init__(*args, **kwargs)
        values = {'study_id': kwargs.get('study_id'),
             'end_date': kwargs.get('end_date', None),
             'start_date': kwargs.get('start_date', None),
             'limit': kwargs.get('limit', None),
             'ordered_by': kwargs.get('ordered_by', 'date'),
             'order_direction': kwargs.get('order_direction', 'descending'),
             'participant_ids': participant_ids,
             'fields': fields}



def _validate_and_coerce_query(**kwargs):
    # functionality should move to an as_python function?
    fields = kwargs.get('fields', '')
    fields = fields.split(',')
    if fields == ['']:
        fields = []

    participant_ids = kwargs.get('participant_ids', '')
    participant_ids = participant_ids.split(',')
    if participant_ids == ['']:
        participant_ids = Participant.objects.none()

    query = {'study_id': kwargs.get('study_id'),
             'end_date': kwargs.get('end_date', None),
             'start_date': kwargs.get('start_date', None),
             'limit': kwargs.get('limit', None),
             'ordered_by': kwargs.get('ordered_by', 'date'),
             'order_direction': kwargs.get('order_direction', 'descending'),
             'participant_ids': participant_ids,
             'fields': fields}

    form = ApiQueryForm(query)
    if not form.is_valid():
        return form.errors.as_json(), None
    return None, form.cleaned_data
