from __future__ import print_function

import jinja2
import os
from flask import abort, Flask, render_template
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__, static_folder="frontend/static")
app.secret_key = "the quick brown fox jumps over the lazy dog"
app.jinja_loader = jinja2.ChoiceLoader([app.jinja_loader, jinja2.FileSystemLoader("frontend/templates")])

# this appears to be unnecessary for debug mode
# app.wsgi_app = ProxyFix(app.wsgi_app)


# setting a route of "/" causes wsgi to break
# @app.route("/")
@app.route("/<page>")
@app.route("/<page>.html")
def render_standard_page(page):
    print("endpoint hit: '%s'" % page)
    if "upload" in page:
        print("treating as an upload endpoint just in case...")
        return abort(500)
    return render_template("downtime.html")


# this ends fixing handling the "missing" "/" endpoint issue above.
@app.errorhandler(404)
def e404(e):
    print("endpoint miss: '%s'" % e)
    return render_template("downtime.html"), 404


@app.route('/upload', methods=['POST', "GET"])
@app.route('/upload/ios/', methods=['GET', 'POST'])
@app.route('/register_user', methods=['GET', 'POST'])
@app.route('/register_user/ios/', methods=['GET', 'POST'])
@app.route('/set_password', methods=['GET', 'POST'])
@app.route('/set_password/ios/', methods=['GET', 'POST'])
@app.route('/download_surveys', methods=['GET', 'POST'])
@app.route('/download_surveys/ios/', methods=['GET', 'POST'])
def fake_upload_endpoint():
    """
    These endpoints have return code based behavior and therefore require a 500 error to "work"
    correctly.
    """
    return abort(500)


print("\nRunning in downtime mode.\n")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", "8080")), debug=True)
