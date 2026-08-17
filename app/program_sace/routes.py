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
    return render_template("program_sace/compliance/index.html")

@sace_bp.route("/sace/compliance/annexure_a")
@login_required
def annexure_a():
    return render_template("program_sace/compliance/annexure_a.html")

@sace_bp.route("/sace/compliance/annexure_b")
@login_required
def annexure_b():
    return render_template("program_sace/compliance/annexure_b.html")

@sace_bp.route("/sace/compliance/annexure_c")
@login_required
def annexure_c():
    return render_template("program_sace/compliance/annexure_c.html")

@sace_bp.route("/sace/compliance/annexure_d")
@login_required
def annexure_d():
    return render_template("program_sace/compliance/annexure_d.html")

@sace_bp.route("/sace/compliance/annexure_e")
@login_required
def annexure_e():
    return render_template("program_sace/compliance/annexure_e.html")

@sace_bp.route("/sace/compliance/evidence")
@login_required
def compliance_evidence():
    import datetime
    today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    return render_template("program_sace/compliance/evidence.html", today=today)
