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
from .reading import routes as _reading     # /admin/reading/...
from .billing import routes as _billing     # /admin/billing/...
from .home import routes as _home     # /admin/home/...
from .loss import routes as _loss     # /admin/loss/...
from .general import routes as _general
from .security import routes as _security
from .security import modules_control as _modules_control
