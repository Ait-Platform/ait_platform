from flask import Blueprint


budget_bp = Blueprint("budget_bp", __name__, url_prefix="/budget")
from . import routes  # noqa

