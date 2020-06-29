from flask.views import MethodView


class SummaryStatisticDailyStudyView(MethodView):
    """
    API endpoint for retrieving SummaryStatisticsDaily objects for a study.
    """
    
    def get(self, study_id):
        return 'ok'
    
    @classmethod
    def register_urls(cls, app):
        """
        Register this class' URLs with Flask
        """
        app.add_url_rule(
            '/api/v0/studies/<string:study_id>/summary-statistics/daily',
            view_func=cls.as_view('summary_statistics_daily_study_view'),
        )
