from flask import abort, render_template, current_app
from app.extensions import db
from app.admin import admin_bp
from jinja2.exceptions import TemplateNotFound

@admin_bp.route("/programs/", endpoint="programs_index")
def programs_index():
    from app.models.auth import AuthSubject
    subjects = AuthSubject.query.filter(
        AuthSubject.is_active == 1,
        ~AuthSubject.slug.in_(['admin', 'admin_general', 'admin-general'])
    ).order_by(AuthSubject.name).all()
    return render_template("admin/programs/index.html", subjects=subjects)

@admin_bp.route("/<subject>/", endpoint="subject_dashboard")
def subject_dashboard(subject: str):
    from app.models.auth import AuthSubject
    subject = (subject or "").lower().strip()
    
    subj_obj = AuthSubject.query.filter_by(slug=subject, is_active=1).first()
    if not subj_obj:
        abort(404)
        
    try:
        return render_template(f"admin/programs/{subject}/dashboard.html", subject=subject, subject_name=subj_obj.name)
    except TemplateNotFound:
        return render_template("admin/programs/fallback_dashboard.html", subject=subject, subject_name=subj_obj.name)
