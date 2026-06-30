from flask import Blueprint

mechanic_bp = Blueprint(
    "mechanic_bp",
    __name__,
    template_folder="../templates",
    static_folder="../static"
)

from . import routes
