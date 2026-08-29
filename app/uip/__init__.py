from flask import Blueprint

uip_bp = Blueprint("uip_bp", __name__, url_prefix="/uip")

from . import routes
