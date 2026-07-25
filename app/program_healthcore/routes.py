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
