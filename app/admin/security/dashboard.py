from flask import render_template
from app.admin import admin_bp


@admin_bp.route("/security", endpoint="security_dashboard")
def security_dashboard():
    return render_template("admin/security/dashboard.html")