from flask import render_template
from flask.views import MethodView
from flask_cors import cross_origin

from api.tableau_api.constants import FIELD_TYPE_MAP, SERIALIZABLE_FIELD_NAMES
from database.tableau_api_models import SummaryStatisticDaily


class WebDataConnector(MethodView):

    path = '/api/v0/studies/<string:study_object_id>/summary-statistics/daily/wdc'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # build the columns datastructure for tableau to enumerate the format of the API data
        fields = (f for f in SummaryStatisticDaily._meta.fields if f.name in SERIALIZABLE_FIELD_NAMES)
        self.cols = '[\n'

        for field in fields:
            for (py_type, tableau_type) in FIELD_TYPE_MAP:
                if isinstance(field, py_type):
                    self.cols += f"{{id: '{field.name}', dataType: {tableau_type},}},\n"
                    # ex line: {id: 'participant_id', dataType: tableau.dataTypeEnum.int,},
                    break
            else:
                # if the field is not recognized, supply it to tableau as a string type
                self.cols += f"{{id: '{field.name}', dataType: tableau.dataTypeEnum.string,}},\n"

        self.cols += '];'

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
        return render_template('wdc.html', study_id=study_id, cols=self.cols)
