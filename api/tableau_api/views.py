from api.tableau_api.base import TableauApiView
from database.tableau_api_models import SummaryStatisticDaily
from django.core import serializers
from django.shortcuts import HttpResponse
from flask import request
from werkzeug.exceptions import abort

class DatebaseQueryFailed(Exception): pass

class SummaryStatisticDailyStudyView(TableauApiView):
    """
    API endpoint for retrieving SummaryStatisticsDaily objects for a study.
    """
    path = '/api/v0/studies/<string:study_id>/summary-statistics/daily'

    def get(self, study_id):
        params = request.values
        queryset = self._query_database(study_id=study_id, **params)

        JSONSerializer = serializers.get_serializer("json")
        json_serializer = JSONSerializer()
        json_serializer.serialize(queryset)
        data = json_serializer.getvalue()  # possibly useful to optimize to write to a file directly/stream?
        return data

    def _query_database(self, study_id, end_date=None, start_date=None, fields=None, limit=None, ordered_by='Date',
            order_direction='descending', participant_ids=None):

        # queryset = SummaryStatisticDaily.objects.filter(study__id=study_id)  # TODO (CD) lots more filters

        if order_direction.lower() == 'descending':
            ordered_by = '-' + ordered_by
        elif order_direction.lower() == 'ascending':
            pass
        else:
            pass  # error here?

        queryset = SummaryStatisticDaily.objects.filter(study__id=study_id).order_by(ordered_by)
        if end_date:
            queryset.filter(date__lte=end_date)
        if start_date:
            queryset.filter(date__gte=start_date)
        if limit:
            queryset = queryset[:limit]

        return queryset





# Todo (CD): Implement SummaryStatisticDailyParticipantView
