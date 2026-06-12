from flask import Blueprint, redirect, url_for, session
from app.models.auth import AuthSubject
from app.quote.routes import get_baton_context


program_bp = Blueprint("program_bp", __name__)

@program_bp.route("/program/<subject_slug>/start")
def program_entry(subject_slug):
    subj = AuthSubject.query.filter_by(slug=subject_slug).first()
    if not subj:
        return redirect(url_for("public_bp.welcome"))

    baton = get_baton_context()
    baton["subject_slug"] = subj.slug
    baton["subject_id"] = subj.id
    session["baton"] = baton

    return redirect(url_for("public_bp.welcome", subject=subj.slug))