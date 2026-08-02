from flask import Blueprint, redirect, request, url_for
from flask_login import current_user
from app.utils.roles import is_admin

# One admin blueprint for the whole admin area
admin_bp = Blueprint("admin_bp", __name__, url_prefix="/admin")

@admin_bp.before_request
def _guard():
    # Allow authenticated property managers to access billing statement routes
    if request.path.startswith('/admin/billing/') and current_user.is_authenticated:
        return None
        
    if not is_admin():
        return redirect(url_for("public_bp.welcome"))

from app.admin import core                          # /admin/
from .general import routes as _general
from .security import routes as _security
from .security import modules_control as _modules_control

# Programs (Subjects)
from .programs import routes as _programs
from .programs import chapters as _chapters
from .programs import lessons as _lessons
from .programs import assessments as _assessments
from .programs.reading import routes as _reading
from .programs.billing import routes as _billing
from .programs.loss import routes as _loss
from .programs.grade12_core_math import routes as _math

from .programs.sms import routes as _sms
