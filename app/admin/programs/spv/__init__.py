from flask import Blueprint

spv_admin_bp = Blueprint(
    "spv_admin_bp",
    __name__,
    url_prefix="/admin/spv",
)

from . import routes  # noqa