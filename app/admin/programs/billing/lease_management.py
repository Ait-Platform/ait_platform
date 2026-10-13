"""Administrator-only, read-only access to the existing PM lease review."""
from io import BytesIO
from pathlib import Path

from flask import abort, current_app, render_template, request, send_file
from flask_login import current_user
from jinja2 import ChoiceLoader, FileSystemLoader
from sqlalchemy import String, cast, func, select
from sqlalchemy.exc import SQLAlchemyError

from app.admin import admin_bp
from app.extensions import db
from app.models.auth import ApprovedAdmin, User
from app.program_billing.lease_documents import (
    PDF_NAME, WORD_NAME, ReviewDocumentUnavailable, read_review_pair,
)

STATUS = "DRAFT — NOT FOR SIGNATURE OR ISSUANCE"
ENDPOINTS = {"admin_bp.pm_lease_management", "admin_bp.pm_lease_preview", "admin_bp.pm_lease_word"}


def require_lease_admin():
    # Fresh scalar query: neither cached roles nor session admin flags grant access.
    if not current_user.is_authenticated:
        abort(403)
    statement = (select(User.id)
        .join(ApprovedAdmin, func.lower(ApprovedAdmin.email) == func.lower(User.email))
        .where(User.id == current_user.id, User.is_active == 1, cast(ApprovedAdmin.active, String).in_(("true", "1"))))
    try:
        allowed = db.session.execute(statement.execution_options(autoflush=False)).scalar_one_or_none()
    except SQLAlchemyError:
        abort(503, description="Administrative access could not be verified.")
    if allowed is None:
        abort(403)


def render_pm_template(name, **context):
    # Local factory points at app/templates; PM templates also live at repo/templates.
    # Keep this fallback scoped to PM instead of changing the platform loader.
    environment = current_app.jinja_env.overlay(loader=ChoiceLoader([
        current_app.jinja_env.loader,
        FileSystemLoader(str(Path(current_app.root_path).parent / "templates")),
    ]))
    return render_template(environment.get_template(name), **context)


@admin_bp.after_request
def protect_lease_responses(response):
    if request.endpoint in ENDPOINTS:
        response.headers["Cache-Control"] = "private, no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Content-Security-Policy"] = "frame-ancestors 'self'"
    return response


@admin_bp.get("/billing/leases", endpoint="pm_lease_management")
def lease_management():
    require_lease_admin()
    available = True
    try:
        read_review_pair()
    except ReviewDocumentUnavailable:
        available = False
    return render_pm_template("program_billing/lease_management.html",
        document_name=WORD_NAME, status=STATUS, available=available), 200 if available else 503


def deliver_review(filename, mimetype, *, attachment):
    require_lease_admin()
    try:
        content = read_review_pair()[filename]
    except ReviewDocumentUnavailable:
        abort(503, description="The current lease review is temporarily unavailable.")
    return send_file(BytesIO(content), mimetype=mimetype, as_attachment=attachment,
        download_name=filename, conditional=False, etag=False, max_age=0)


@admin_bp.get("/billing/leases/preview", endpoint="pm_lease_preview")
def lease_preview():
    return deliver_review(PDF_NAME, "application/pdf", attachment=False)


@admin_bp.get("/billing/leases/word", endpoint="pm_lease_word")
def lease_word():
    return deliver_review(WORD_NAME,
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document", attachment=True)
