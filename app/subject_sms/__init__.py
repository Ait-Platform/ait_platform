from flask import Blueprint

sms_subject_bp = Blueprint(
    "sms_subject_bp",
    __name__,
    url_prefix="/sms",
)

from . import routes  # noqa
