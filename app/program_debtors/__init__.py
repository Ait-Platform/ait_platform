from flask import Blueprint

debtors_bp = Blueprint('debtors_bp', __name__)

from . import routes
