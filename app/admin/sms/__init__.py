from flask import Blueprint

sms_admin_bp = Blueprint(
    "sms_admin_bp",
    __name__,
    url_prefix="/admin/sms",
)

from . import routes  # noqa
