from flask import Blueprint


quote_bp = Blueprint("quote_bp", __name__, url_prefix="/flow")

from . import routes  # noqa