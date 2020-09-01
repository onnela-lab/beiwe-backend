from flask import render_template, Blueprint
from flask_cors import cross_origin

from authentication.admin_authentication import authenticate_researcher_login

tableau_pages = Blueprint('tableau_pages', __name__)


@tableau_pages.route('/tableau', methods=['GET'])
@authenticate_researcher_login
@cross_origin()
def tableau_view():
    return render_template('tableau.html')


@tableau_pages.route('/tableau_embed', methods=['GET'])
@authenticate_researcher_login
@cross_origin()
def tableau_embed_view():
    return render_template('tableau_embed.html')
