"""Property Management lease workspace with property-scoped private review access."""
from io import BytesIO
from pathlib import Path

from flask import abort, current_app, render_template, request, send_file
from flask_login import current_user
from jinja2 import ChoiceLoader, FileSystemLoader
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.program_billing.routes import billing_bp
from app.extensions import db
from app.models.auth import User
from app.models.billing import BilProperty
from app.program_billing.lease_documents import (
    PDF_NAME, WORD_NAME, ReviewDocumentUnavailable, read_review_pair,
)

STATUS = "DRAFT — NOT FOR SIGNATURE OR ISSUANCE"
ENDPOINTS = {"billing_bp.pm_lease_management", "billing_bp.pm_lease_preview", "billing_bp.pm_lease_word"}


def require_pm_user():
    if not current_user.is_authenticated:
        abort(403)
    statement = select(User.id).where(User.id == current_user.id, User.is_active == 1)
    try:
        allowed = db.session.execute(statement.execution_options(autoflush=False)).scalar_one_or_none()
    except SQLAlchemyError:
        abort(503, description="Property Management access could not be verified.")
    if allowed is None:
        abort(403)


def authorised_review_property():
    # Explicit R01 association only; never infer a property from a user's identity.
    configured = str(current_app.config.get("PM_LEASE_REVIEW_PROPERTY_ID", "")).strip()
    if not configured.isdecimal() or int(configured) <= 0:
        current_app.logger.warning("R01 lease association unavailable: PM_LEASE_REVIEW_PROPERTY_ID is missing or invalid.")
        return None
    statement = select(BilProperty.id).where(BilProperty.id == int(configured),
        BilProperty.manager_id == current_user.id, BilProperty.is_archived == False)
    try:
        return db.session.execute(statement.execution_options(autoflush=False)).scalar_one_or_none()
    except SQLAlchemyError:
        abort(503, description="Property access could not be verified.")


def render_pm_template(name, **context):
    # Local factory points at app/templates; PM templates also live at repo/templates.
    # Keep this fallback scoped to PM instead of changing the platform loader.
    environment = current_app.jinja_env.overlay(loader=ChoiceLoader([
        current_app.jinja_env.loader,
        FileSystemLoader(str(Path(current_app.root_path).parent / "templates")),
    ]))
    return render_template(environment.get_template(name), **context)


@billing_bp.after_request
def protect_lease_responses(response):
    if request.endpoint in ENDPOINTS:
        response.headers["Cache-Control"] = "private, no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Content-Security-Policy"] = "frame-ancestors 'self'"
    return response


@billing_bp.get("/billing/leases", endpoint="pm_lease_management")
def lease_management():
    require_pm_user()
    associated = authorised_review_property() is not None
    available = associated
    missing = False
    if associated:
        try:
            read_review_pair()
        except ReviewDocumentUnavailable:
            available = False
            missing = True
    return render_pm_template("program_billing/lease_management.html",
        document_name=WORD_NAME if associated else None, status=STATUS,
        available=available, associated=associated), 503 if missing else 200


def deliver_review(filename, mimetype, *, attachment):
    require_pm_user()
    if authorised_review_property() is None:
        abort(404)
    try:
        content = read_review_pair()[filename]
    except ReviewDocumentUnavailable:
        abort(503, description="The current lease review is temporarily unavailable.")
    return send_file(BytesIO(content), mimetype=mimetype, as_attachment=attachment,
        download_name=filename, conditional=False, etag=False, max_age=0)


@billing_bp.get("/billing/leases/preview", endpoint="pm_lease_preview")
def lease_preview():
    return deliver_review(PDF_NAME, "application/pdf", attachment=False)


@billing_bp.get("/billing/leases/word", endpoint="pm_lease_word")
def lease_word():
    return deliver_review(WORD_NAME,
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document", attachment=True)
