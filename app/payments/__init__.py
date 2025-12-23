from flask import Blueprint

payment_bp = Blueprint("payment_bp", __name__, url_prefix="/payments")

from . import routes  # noqa
