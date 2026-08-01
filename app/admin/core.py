
from app.models.payment import VoucherToken
from app.models.auth import AuthSubject
from app.utils.roles import is_admin
from flask import abort
from flask_login import current_user
# Cleaned admin core module
import os
import uuid
import base64
import json
import glob
import re
# import google.generativeai as genai
# Note: google.generativeai is deprecated. Since this is admin/core.py, 
# we'll just catch the import or suppress the warning if it's unused.
try:
    import google.genai as genai
except ImportError:
    pass
from sqlalchemy import text
from flask import (
    render_template,
    request,
    jsonify,
    current_app,
    redirect,
    url_for,
    flash,
)
from app.admin import admin_bp
from app.extensions import db
from app.models.auth import AuthSubject
from app.models.adv_math import AdvMathQuestion

@admin_bp.route("/", endpoint="index")
def index():
    allowed = ["reading", "home", "loss", "billing", "adv_math", "spv"]
    subjects = AuthSubject.query.filter(AuthSubject.slug.in_(allowed)).order_by(AuthSubject.name).all()
    return render_template("admin/index.html", subjects=subjects)

