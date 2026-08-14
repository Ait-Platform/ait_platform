from flask import Blueprint

cptd_bp = Blueprint("cptd_bp", __name__, template_folder="../../templates")

from . import routes
