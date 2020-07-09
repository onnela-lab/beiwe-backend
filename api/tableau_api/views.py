from api.tableau_api.base import TableauApiView
from database.tableau_api_models import SummaryStatisticDaily
from django.core import serializers
from django.shortcuts import HttpResponse
from flask import request
from datetime import date, datetime
from dateutil.parser import parse
from django.core.exceptions import FieldError


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
        params = request.values
        try:
            queryset = self._query_database(study_id=study_id, **params)
        except (TypeError, FieldError) as err:  # unexpected keyword
            return self._render_error(err)

        JSONSerializer = serializers.get_serializer("json")
        json_serializer = JSONSerializer()
        json_serializer.serialize(queryset, fields=params.get('fields', None))

        data = json_serializer.getvalue()  # possibly useful to optimize to write to a file directly/stream?
        return data

    def _query_database(self, study_id, end_date=None, start_date=None, limit=None, ordered_by='date',
                        order_direction='descending', participant_ids=None, **_):
        """
        note: the 'fields' parameter is also used by API, but handled directly in the get function
        """
        if order_direction.lower() == 'descending':
            ordered_by = '-' + ordered_by
        elif order_direction.lower() == 'ascending':
            pass
        else:
            raise InvalidInput('If provided, the "ordered-direction" parameter is expected to have a value of '
                               '"ascending" or "descending"')

        queryset = SummaryStatisticDaily.objects.filter(study__id=study_id)
        if participant_ids:
            participant_ids = participant_ids.split(',')
            queryset = queryset.filter(participant__patient_id__in=participant_ids)
        if end_date:
            queryset = queryset.filter(date__lte=parse(end_date))
        if start_date:
            queryset = queryset.filter(date__gte=parse(start_date))
        queryset = queryset.order_by(ordered_by)
        if limit:
            queryset = queryset[:int(limit)]  # consider edge cases + queryset limit
        return queryset

    def _render_error(self, err):
        return 1  # TODO


# Todo (CD): Implement SummaryStatisticDailyParticipantView
