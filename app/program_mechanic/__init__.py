from flask import Blueprint

mechanic_bp = Blueprint(
    "mechanic_bp",
    __name__,
    template_folder="../templates",
    static_folder="../static"
)

from . import routes
from app.protrade_boq_time import register_routes
register_routes(mechanic_bp)
from app.protrade_pricing import register_routes as register_pricing_routes
register_pricing_routes(mechanic_bp)
