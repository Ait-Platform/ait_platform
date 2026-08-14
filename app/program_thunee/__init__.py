from flask import Blueprint

thunee_bp = Blueprint('thunee_bp', __name__, template_folder='../../templates', static_folder='../../static')

from . import routes
