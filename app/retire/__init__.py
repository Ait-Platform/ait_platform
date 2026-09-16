from flask import Blueprint

retire_bp = Blueprint(
    "retire_bp", __name__, url_prefix="/retire", template_folder="../../templates"
)

from . import routes  # noqa: E402, F401
