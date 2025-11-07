from flask import render_template
from .. import admin_bp

@admin_bp.route("/home/", endpoint="home_home")
def home_home():
    return render_template("admin/home/index.html")