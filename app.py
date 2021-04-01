import os
from datetime import datetime

import jinja2
from flask import Flask, redirect, render_template
from flask_cors import CORS
from raven.contrib.flask import Sentry
from werkzeug.middleware.proxy_fix import ProxyFix

from api import (admin_api, copy_study_api, dashboard_api, data_access_api, mobile_api,
    other_researcher_apis, participant_administration, push_notifications_api, study_api,
    survey_api)
from api.tableau_api.views import SummaryStatisticDailyStudyView
from api.tableau_api.web_data_connector import WebDataConnector
from authentication.admin_authentication import is_logged_in
from config.settings import SENTRY_ELASTIC_BEANSTALK_DSN, SENTRY_JAVASCRIPT_DSN
from libs.security import set_secret_key
from libs.sentry import normalize_sentry_dsn
from pages import (admin_pages, data_access_web_form, login_pages, mobile_pages, survey_designer,
                   system_admin_pages, forest_pages)

# Flask App
app = Flask(__name__, static_folder="frontend/static")
app.jinja_loader = jinja2.ChoiceLoader(
    [app.jinja_loader, jinja2.FileSystemLoader("frontend/templates")]
)
set_secret_key(app)
app.wsgi_app = ProxyFix(app.wsgi_app)

CORS(app)

# Flask Blueprints
app.register_blueprint(login_pages.login_pages)
app.register_blueprint(mobile_api.mobile_api)
app.register_blueprint(admin_pages.admin_pages)
app.register_blueprint(mobile_pages.mobile_pages)
app.register_blueprint(system_admin_pages.system_admin_pages)
app.register_blueprint(forest_pages.forest_pages)
app.register_blueprint(survey_designer.survey_designer)
app.register_blueprint(admin_api.admin_api)
app.register_blueprint(participant_administration.participant_administration)
app.register_blueprint(survey_api.survey_api)
app.register_blueprint(study_api.study_api)
app.register_blueprint(data_access_api.data_access_api)
app.register_blueprint(data_access_web_form.data_access_web_form)
app.register_blueprint(other_researcher_apis.other_researcher_apis)
app.register_blueprint(copy_study_api.copy_study_api)
app.register_blueprint(dashboard_api.dashboard_api)
app.register_blueprint(push_notifications_api.push_notifications_api)
SummaryStatisticDailyStudyView.register_urls(app)
WebDataConnector.register_urls(app)

# Jinja
app.jinja_env.globals['current_year'] = datetime.now().strftime('%Y')


# Sentry is not required, that was too much of a hassle
if SENTRY_ELASTIC_BEANSTALK_DSN:
    sentry = Sentry(app, dsn=normalize_sentry_dsn(SENTRY_ELASTIC_BEANSTALK_DSN))


@app.route("/<page>.html")
def strip_dot_html(page):
    # Strips away the dot html from pages
    return redirect("/%s" % page)


# this would be called every page load in the context processor
DERIVED_DSN = normalize_sentry_dsn(SENTRY_JAVASCRIPT_DSN)


@app.context_processor
def inject_dict_for_all_templates():
    return {"SENTRY_JAVASCRIPT_DSN": DERIVED_DSN}


# Extra Production settings
if not __name__ == '__main__':
    # Points our custom 404 page (in /frontend/templates) to display on a 404 error
    @app.errorhandler(404)
    def e404(e):
        return render_template("404.html", is_logged_in=is_logged_in()), 404


# Extra Debugging settings
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", "8080")), debug=True)
