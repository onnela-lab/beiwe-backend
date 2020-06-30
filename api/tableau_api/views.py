from api.tableau_api.base import TableauApiView


class SummaryStatisticDailyStudyView(TableauApiView):
    """
    API endpoint for retrieving SummaryStatisticsDaily objects for a study.
    """
    path = '/api/v0/studies/<string:study_id>/summary-statistics/daily'

    def get(self, study_id):
        # Todo (CD): Implement SummaryStatisticDaily database model
        # Todo (CD): Implement this logic
        return 'ok'


# Todo (CD): Implement SummaryStatisticDailyParticipantView
