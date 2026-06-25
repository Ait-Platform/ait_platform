from flask import Blueprint

hds_bp = Blueprint('hds_bp', __name__, template_folder='../../templates', url_prefix='/hds')

from . import routes
