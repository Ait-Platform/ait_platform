"""HOME-only authority and examination services."""
import hashlib
import secrets
from datetime import timedelta
from pathlib import Path
from flask import abort, current_app, session
from flask_login import current_user
from app.extensions import db
from app.models.sace_home import (HomeController, HomeProvisioning, HomeInvitation,
    HomePledge, HomeAssignment, HomeDocument, HomeDocumentVersion, HomeEvidence, now)

SUBJECT = "sace_home_endorsement"
PLEDGE_VERSION = "home-ip-v1"
PLEDGE_TEXT = (
    "By accessing HOME - Hands-On Math Education for SACE endorsement examination, "
    "you acknowledge that the digital framework, visual mapping, and associated "
    "teaching methodologies are the protected Intellectual Property of the Archoney "
    "Institute of Technology (AIT). Access is provided solely for the purpose of "
    "SACE endorsement evaluation. No part of the interactive platform or its "
    "proprietary methods may be copied, reproduced, or distributed."
)
ITEMS = {
    "summary": "Activity Summary",
    "timetable": "HOME programme / timetable",
    "participant_manual": "HOME Workshop / Participant Manual",
    "facilitator_manual": "HOME Facilitator Manual",
    "experience": "HOME practical / learning experience",
    "assessment": "Assessment tools / evidence",
    "monitoring": "Monitoring / evaluation evidence",
    "certificate": "Applicable HOME certificate evidence",
}
DOCUMENT_ITEMS = set(ITEMS) - {"summary", "experience"}


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def controller():
    if not current_user.is_authenticated:
        return None
    return HomeController.query.filter_by(user_id=current_user.id, active=True).first()


def require_controller():
    row = controller()
    if row is None:
        abort(403, description="HOME controller authority is required.")
    return row


def issue_provisioning(email, issued_by, days=7):
    token = secrets.token_urlsafe(32)
    row = HomeProvisioning(token_hash=digest(token), email=email.strip().lower(),
        issued_by=issued_by, expires_at=now() + timedelta(days=days))
    db.session.add(row)
    db.session.flush()
    return token


def provisioning(lock=False):
    token = session.get("sace_home_provisioning_token", "")
    query = HomeProvisioning.query.filter_by(token_hash=digest(token))
    row = query.with_for_update().populate_existing().first() if lock else query.first()
    if row is None or row.claimed_at or row.expires_at <= now():
        abort(403, description="A valid, unused HOME provisioning link is required.")
    return row


def invitation(code=None, lock=False):
    code = (code if code is not None else session.get("sace_home_pending_code", "")).strip().upper()
    if not code.startswith("HOME-"):
        abort(400, description="Invalid HOME access code.")
    query = HomeInvitation.query.filter_by(code_hash=digest(code))
    row = query.with_for_update().populate_existing().first() if lock else query.first()
    if row is None or row.status != "unclaimed" or row.expires_at <= now():
        abort(400, description="HOME access code is invalid, expired or already claimed.")
    owner = db.session.get(HomeController, row.controller_id)
    if owner is None or not owner.active:
        abort(403, description="This HOME invitation is no longer available.")
    return row


def issue_invitation(owner):
    code = "HOME-" + secrets.token_hex(12).upper()
    row = HomeInvitation(controller_id=owner.id, code_hash=digest(code),
        expires_at=now() + timedelta(days=14))
    db.session.add(row)
    db.session.flush()
    return code


def consent(role, context_id):
    value = session.get("sace_home_pledge_" + role, {})
    if (value.get("context_id") != context_id or value.get("version") != PLEDGE_VERSION
            or not value.get("signature")):
        abort(400, description="Review and sign the HOME IP pledge first.")
    return value


def persist_pledge(role, row):
    from datetime import datetime
    value = consent(role, row.id)
    pledge = HomePledge(user_id=current_user.id, role=role,
        signature=value["signature"], version=PLEDGE_VERSION, text_hash=digest(PLEDGE_TEXT),
        accepted_at=datetime.fromisoformat(value["accepted_at"]),
        provisioning_id=row.id if role == "controller" else None,
        invitation_id=row.id if role == "auditor" else None)
    db.session.add(pledge)
    return pledge


def complete_provisioning():
    row = provisioning(lock=True)
    if current_user.email.strip().lower() != row.email:
        abort(403, description="Sign in with the email named in the HOME provisioning invitation.")
    # Serialise repeated grants for the same shared identity without granting any platform role.
    from app.models.auth import User
    User.query.filter_by(id=current_user.id).with_for_update().one()
    grant = HomeController.query.filter_by(user_id=current_user.id).first()
    if grant is not None and not grant.active:
        abort(403, description="HOME authority has been revoked.")
    persist_pledge("controller", row)
    if grant is None:
        db.session.add(HomeController(user_id=current_user.id))
    row.claimed_by, row.claimed_at = current_user.id, now()
    db.session.commit()
    session.pop("sace_home_provisioning_token", None)
    session.pop("sace_home_pledge_controller", None)


def claim():
    row = invitation(lock=True)
    owner = db.session.get(HomeController, row.controller_id)
    if owner.user_id == current_user.id:
        abort(403, description="A HOME controller cannot examine their own invitation.")
    persist_pledge("auditor", row)
    assignment = HomeAssignment(invitation_id=row.id, auditor_id=current_user.id)
    db.session.add(assignment)
    row.status, row.claimed_at = "claimed", now()
    db.session.flush()
    record(assignment, "pledge", "accepted", {"version": PLEDGE_VERSION})
    record(assignment, "assignment", "claimed")
    db.session.commit()
    session.pop("sace_home_pending_code", None)
    session.pop("sace_home_pledge_auditor", None)
    return assignment


def assignment(assignment_id, lock=False, writable=False):
    query = HomeAssignment.query.filter_by(id=assignment_id, auditor_id=current_user.id)
    row = query.with_for_update().populate_existing().first() if lock else query.first()
    if row is None or row.status == "revoked":
        abort(403, description="This HOME assignment is not available to you.")
    if writable and row.status != "active":
        abort(403, description="This HOME assignment is closed.")
    return row


def record(row, item, event, details=None, version=None):
    evidence = HomeEvidence(assignment_id=row.id, actor_id=current_user.id,
        item=item, event=event, details=details or {}, document_version_id=version)
    db.session.add(evidence)
    db.session.flush()
    return evidence


def latest_version(kind):
    return (HomeDocumentVersion.query.join(HomeDocument)
        .filter(HomeDocument.kind == kind)
        .order_by(HomeDocumentVersion.published_at.desc(), HomeDocumentVersion.id.desc()).first())


def document_path(version):
    root = Path(current_app.config.get("SACE_HOME_DOCUMENT_ROOT",
        Path(current_app.instance_path) / "sace_home_documents")).resolve()
    path = (root / version.storage_key).resolve()
    if not path.is_relative_to(root) or path == root or not path.is_file():
        abort(404, description="HOME evidence file is not available.")
    if hashlib.sha256(path.read_bytes()).hexdigest() != version.sha256:
        abort(409, description="HOME evidence file does not match its published version.")
    return path


def examined(row, item, version=None):
    return HomeEvidence.query.filter_by(assignment_id=row.id, item=item,
        event="examined", document_version_id=version).first() is not None


def board_items(row):
    values = []
    for kind, title in ITEMS.items():
        version = latest_version(kind) if kind in DOCUMENT_ITEMS else None
        available = kind == "summary" or version is not None
        values.append(dict(kind=kind, title=title, version=version, available=available,
            examined=available and examined(row, kind, version.id if version else None)))
    return values


def missing(row):
    # Experience cannot satisfy completion until the actual HOME wrapper is implemented.
    return [item["title"] for item in board_items(row) if not item["examined"]]


def auth_destination():
    """Explicit HOME continuation only; never consumes LITRE session context."""
    if session.get("sace_home_pending_code"):
        return "home_sace_bp.claim_assignment"
    if session.get("sace_home_provisioning_token"):
        return "home_sace_bp.provision"
    return "home_sace_bp.entry"


def publish_document(owner, kind, version_label, storage_key, manifest):
    """Append a controlled version; existing versions/evidence are never rewritten."""
    if kind not in DOCUMENT_ITEMS or not version_label.strip() or not isinstance(manifest, dict):
        raise ValueError("Provide a supported HOME document kind, version and source manifest.")
    if kind in {"participant_manual", "facilitator_manual"}:
        approval = manifest.get("source_approval") or {}
        if (not approval.get("production_parity_checked") or not approval.get("approved_by")
                or not approval.get("approved_at") or not manifest.get("source_sha256")
                or manifest.get("gaps") or manifest.get("builder_version") != "home-source-snapshot-v1"):
            raise ValueError("HOME manuals require a complete source manifest and explicit production-parity/source approval.")
        source = manifest.get("source")
        if not source:
            raise ValueError("The approved HOME manual source snapshot is missing.")
        from .manual_sources import canonical
        if digest(canonical(source)) != manifest["source_sha256"]:
            raise ValueError("The HOME source snapshot does not match its approved digest.")
    root = Path(current_app.config.get("SACE_HOME_DOCUMENT_ROOT",
        Path(current_app.instance_path) / "sace_home_documents")).resolve()
    path = (root / storage_key).resolve()
    if not path.is_relative_to(root) or not path.is_file() or path.suffix.lower() != ".pdf":
        raise ValueError("Place the PDF inside the private HOME document root first.")
    content = path.read_bytes()
    if not content.startswith(b"%PDF-"):
        raise ValueError("The HOME evidence file is not a PDF.")
    # Serialise publication so simultaneous calls cannot create the same document kind.
    HomeController.query.filter_by(id=owner.id, active=True).with_for_update().one()
    db.session.execute(db.text("SELECT pg_advisory_xact_lock(74831029)"))
    document = HomeDocument.query.filter_by(kind=kind).first()
    if document is None:
        document = HomeDocument(kind=kind, title=ITEMS[kind])
        db.session.add(document)
        db.session.flush()
    if HomeDocumentVersion.query.filter_by(document_id=document.id, version=version_label).first():
        raise ValueError("That HOME document version already exists; publish a new version instead.")
    row = HomeDocumentVersion(document_id=document.id, version=version_label,
        storage_key=str(path.relative_to(root)), sha256=hashlib.sha256(content).hexdigest(),
        source_manifest=manifest, approved_by=owner.id)
    db.session.add(row)
    db.session.flush()
    return row
