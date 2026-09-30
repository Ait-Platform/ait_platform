"""HOME endorsement blueprint: registration performs no database writes."""
from flask import Blueprint

home_sace_bp = Blueprint("home_sace_bp", __name__, url_prefix="/sace/home")
from . import routes, cli  # noqa: E402,F401

from .continuation import discard_abandoned
home_sace_bp.before_app_request(discard_abandoned)
