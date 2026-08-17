from flask import Blueprint

sace_bp = Blueprint("sace_bp", __name__, template_folder="../../templates")

from . import routes
