from flask import Blueprint, render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from app import db

healthcore_bp = Blueprint("healthcore_bp", __name__)

@healthcore_bp.route("/program/healthcore")
def healthcore_home():
    return render_template("program_healthcore/welcome.html")

@healthcore_bp.route("/program/healthcore/about")
def healthcore_about():
    return render_template("program_healthcore/about.html")

@healthcore_bp.route("/program/healthcore/pricing")
def healthcore_pricing():
    return render_template("program_healthcore/pricing.html")

@healthcore_bp.route("/program/healthcore/dashboard")
@login_required
def healthcore_dashboard():
    return render_template("program_healthcore/dashboard.html")

# ---------------------------------------------------------
# ENGINE DASHBOARDS
# ---------------------------------------------------------

@healthcore_bp.route("/program/healthcore/engine/laboratory")
@login_required
def laboratory_dashboard():
    return render_template("program_healthcore/laboratory.html")

@healthcore_bp.route("/program/healthcore/engine/medication")
@login_required
def medication_dashboard():
    return render_template("program_healthcore/medication.html")

@healthcore_bp.route("/program/healthcore/engine/nutrition")
@login_required
def nutrition_dashboard():
    return render_template("program_healthcore/nutrition.html")

@healthcore_bp.route("/program/healthcore/engine/imaging")
@login_required
def imaging_dashboard():
    return render_template("program_healthcore/imaging.html")

@healthcore_bp.route("/program/healthcore/engine/lifestyle")
@login_required
def lifestyle_dashboard():
    return render_template("program_healthcore/lifestyle.html")

@healthcore_bp.route("/program/healthcore/engine/timeline")
@login_required
def timeline_dashboard():
    return render_template("program_healthcore/timeline.html")

@healthcore_bp.route("/program/healthcore/engine/risk")
@login_required
def risk_dashboard():
    return render_template("program_healthcore/risk.html")

@healthcore_bp.route("/program/healthcore/engine/correlation")
@login_required
def correlation_dashboard():
    return render_template("program_healthcore/correlation.html")

@healthcore_bp.route("/program/healthcore/engine/reporting")
@login_required
def reporting_dashboard():
    return render_template("program_healthcore/reporting.html")
