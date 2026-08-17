from flask import render_template, request, redirect, url_for, flash, current_app
from flask_login import login_required, current_user
from app.extensions import db
from . import sace_bp

@sace_bp.route("/sace/about")
def sace_about():
    return render_template("program_sace/about.html")

@sace_bp.route("/sace/dashboard")
@login_required
def dashboard():
    return render_template("program_sace/dashboard.html")
