from flask.views import MethodView
from werkzeug.exceptions import abort

from database.study_models import Study


class PermissionDenied(Exception):
    pass


class TableauApiView(MethodView):
    """
    The base class for all Tableau API views that implements authentication and other functionality
    specific to this API.
    """
    def check_permissions(self, *args, study_id=None, participant_id=None, **kwargs):
        if study_id is None:
            raise PermissionDenied('No study id specified')
        if not Study.objects.filter(object_id=study_id).exists():
            raise PermissionDenied('No matching study found')

        # Todo (CD): Implement the rest of this

        return True
    
    def dispatch_request(self, *args, **kwargs):
        """
        Override `super().dispatch_request` to return 404 if a method is not allowed.
        """
        try:
            self.check_permissions(*args, **kwargs)
        except PermissionDenied:
            return abort(404)
        return super().dispatch_request(*args, **kwargs)
    
    @classmethod
    def register_urls(cls, app):
        """
        Register this class' URLs with Flask
        """
        app.add_url_rule(cls.path, view_func=cls.as_view('summary_statistics_daily_study_view'))


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
