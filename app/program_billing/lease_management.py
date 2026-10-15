"""Property Management lease workspace with property-scoped private review access."""
from io import BytesIO
from pathlib import Path
import re

from flask import abort, current_app, render_template, request, send_file
from flask_login import current_user
from jinja2 import ChoiceLoader, FileSystemLoader
from sqlalchemy import select, inspect, literal
from sqlalchemy.exc import SQLAlchemyError

from app.program_billing.routes import billing_bp
from app.extensions import db
from app.models.auth import User
from app.models.billing import BilProperty, BilLease
from app.program_billing.lease_documents import (
    PDF_NAME, WORD_NAME, ReviewDocumentUnavailable, read_review_pair,
)

STATUS = "DRAFT - NOT FOR SIGNATURE OR ISSUANCE"
ENDPOINTS = {"billing_bp.pm_lease_management", "billing_bp.pm_lease_preview", "billing_bp.pm_lease_word"}


def log_lease_database_error(stage, error):
    original = getattr(error, "orig", None)
    sqlstate = getattr(original, "pgcode", None) or getattr(original, "sqlstate", None)
    diagnostic=getattr(original,"diag",None)
    column=getattr(diagnostic,"column_name",None)
    if not column:
        message=getattr(diagnostic,"message_primary","") or ""
        match=re.search(r'column\s+((?:"?[A-Za-z_][A-Za-z_0-9]*"?\.)?"?[A-Za-z_][A-Za-z_0-9]*"?)\s+does not exist',message)
        column=match.group(1).replace(chr(34),'') if match else None
    current_app.logger.error("PM lease database failure stage=%s error=%s sqlstate=%s missing_column=%s",
        stage, type(error).__name__, sqlstate or "unavailable", column or "unconfirmed")


def require_pm_user():
    if not current_user.is_authenticated:
        abort(403)
    statement = select(User.id).where(User.id == current_user.id, User.is_active == 1)
    try:
        allowed = db.session.execute(statement.execution_options(autoflush=False)).scalar_one_or_none()
    except SQLAlchemyError as error:
        log_lease_database_error("active_user", error)
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
    except SQLAlchemyError as error:
        log_lease_database_error("review_association", error)
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
    # Select only Page 1 fields, always scoped to the signed-in manager.
    statement = select(BilProperty.id, BilProperty.name, BilProperty.address).where(
        BilProperty.manager_id == current_user.id, BilProperty.is_archived == False
    ).order_by(BilProperty.name, BilProperty.id)
    try:
        properties = db.session.execute(statement.execution_options(autoflush=False)).mappings().all()
    except SQLAlchemyError as error:
        log_lease_database_error("property_list", error)
        abort(503, description="Properties could not be loaded.")
    review_property_id = authorised_review_property()
    requested = request.args.get("property_id")
    selected = None
    if requested is not None:
        if not requested.isdecimal() or int(requested) <= 0:
            abort(404)
        selected = next((prop for prop in properties if prop["id"] == int(requested)), None)
        if selected is None:
            abort(404)
    elif review_property_id is not None:
        selected = next((prop for prop in properties if prop["id"] == review_property_id), None)
    leases = []
    listing_warning=None
    if selected is not None:
        leases,listing_warning=existing_lease_rows(selected["id"])
    selected_owners=[]
    owner_information_available=False
    if selected is not None:
        from app.program_billing.owner_management import owners_enabled, owner_rows
        owner_information_available=owners_enabled()
        if owner_information_available:selected_owners=owner_rows(selected["id"])
    associated = selected is not None and selected["id"] == review_property_id
    available = associated
    if associated:
        try:
            read_review_pair()
        except ReviewDocumentUnavailable:
            available = False
            current_app.logger.warning("PM lease review unavailable; property selection remains available.")
    return render_pm_template("program_billing/lease_management.html",
        properties=properties, selected_property=selected, leases=leases,
        document_name=WORD_NAME if associated else None, status=STATUS,
        available=available, associated=associated,listing_warning=listing_warning,
        selected_owners=selected_owners,owner_information_available=owner_information_available)



def existing_lease_rows(property_id):
    """Read compatible scalar columns; never infer a missing property link."""
    try:
        with db.session.begin_nested():
            inspector=inspect(db.session.connection())
            if not inspector.has_table('bil_lease'):
                return [],"Existing leases are unavailable: bil_lease is absent. No records were changed."
            columns={c['name'] for c in inspector.get_columns('bil_lease')}
            missing={'id','property_id','start_date','end_date','rent_amount'}-columns
            if missing:
                current_app.logger.warning("PM lease schema mismatch missing_columns=%s",','.join('bil_lease.'+c for c in sorted(missing)))
            if not {'id','property_id'}<=columns:
                return [],"Existing leases are unavailable: missing property authorization linkage ("+', '.join(sorted(missing))+")."
            fields=[BilLease.id]
            for key in ('start_date','end_date','rent_amount'):
                fields.append(getattr(BilLease,key) if key in columns else literal(None).label(key))
            statement=select(*fields).join(BilProperty,BilProperty.id==BilLease.property_id).where(
                BilLease.property_id==property_id,BilProperty.manager_id==current_user.id,
                BilProperty.is_archived==False).order_by(BilLease.id)
            rows=db.session.execute(statement.execution_options(autoflush=False)).mappings().all()
            warning=("Lease summary is incomplete; missing database columns: "+', '.join('bil_lease.'+c for c in sorted(missing))+". Existing data was not changed.") if missing else None
            return rows,warning
    except SQLAlchemyError as error:
        log_lease_database_error("existing_leases",error)
        return [],"Existing leases are temporarily unavailable. Property selection remains available; no lease records were changed."

def deliver_review(filename, mimetype, *, attachment):
    require_pm_user()
    review_property_id = authorised_review_property()
    requested = request.args.get("property_id")
    if (review_property_id is None or (requested is not None and
            (not requested.isdecimal() or int(requested) != review_property_id))):
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
