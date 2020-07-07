from api.tableau_api.base import TableauApiView
from database.tableau_api_models import SchemaTableauAPI
from django.core import serializers
from django.shortcuts import  HttpResponse

class SummaryStatisticDailyStudyView(TableauApiView):
    """
    API endpoint for retrieving SummaryStatisticsDaily objects for a study.
    """
    path = '/api/v0/studies/<string:study_id>/summary-statistics/daily'

    def get(self, study_id, end_date=None, start_date=None, fields=None, limit=None, ordered_by='Date',
            order_direction='descending', participant_ids=None):

        queryset = SchemaTableauAPI.objects.filter(study__id=study_id)  # TODO (CD) lots more filters

        # lots of ways to do this, but this is drawn from the docs
        JSONSerializer = serializers.get_serializer("json")
        json_serializer = JSONSerializer()
        json_serializer.serialize(queryset)
        data = json_serializer.getvalue()  # possibly useful to optimize to write to a file directly
        return HttpResponse(data)


# Todo (CD): Implement SummaryStatisticDailyParticipantView
