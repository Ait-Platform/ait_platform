from flask import Blueprint, session as flask_session, abort, render_template, current_app, make_response
from sqlalchemy import text as sa_text
from app.extensions import db
from datetime import datetime


from flask import render_template
from flask_login import login_required
from . import sms_admin_bp

@sms_admin_bp.get("/")
@login_required
def sms_dashboard():
    # later this will show SGB, Learners, Teachers, Finance, Assets, etc.
    return render_template("admin/sms/dashboard.html")
