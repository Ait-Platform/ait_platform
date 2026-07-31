from flask import abort, render_template
from app.extensions import db
from app.admin import admin_bp

# subjects you support in admin
ALLOWED_SUBJECTS = {"reading", "home", "loss", "billing", "adv_math", "spv"}  # extend as needed


@admin_bp.route("/<subject>/", endpoint="subject_dashboard")
def subject_dashboard(subject: str):
    subject = (subject or "").lower().strip()
    if subject not in ALLOWED_SUBJECTS:
        abort(404)
    return render_template(f"admin/{subject}/dashboard.html", subject=subject)
