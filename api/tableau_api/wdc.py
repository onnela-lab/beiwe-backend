from flask import render_template
from flask.views import MethodView
from flask_cors import cross_origin


class WebDataConnector(MethodView):

    path = '/api/v0/studies/<string:study_id>/summary-statistics/daily/wdc'

    @classmethod
    def register_urls(cls, app):
        """
        Register this class' URLs with Flask
        """
        app.add_url_rule(cls.path, view_func=cls.as_view("web_data_connector_view"))

    @cross_origin()
    def get(self, study_id):
        # for security reasons, no study_id validation occurs here, and no study info is exposed
        # there is necessarily no validation to get to this page. No information should be exposed here
        return render_template('wdc.html',
                               study_id=study_id)