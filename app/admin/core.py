from . import admin_bp
from flask import render_template

@admin_bp.route("/", endpoint="index")
def index():
    # Generic admin landing. (You can keep this simple.)
    return render_template("admin/index.html")
