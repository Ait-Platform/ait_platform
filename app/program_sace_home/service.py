"""HOME-only authority and examination services."""
import hashlib
import secrets
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from flask import abort, current_app, session, g
from flask_login import current_user
from app.extensions import db
from app.models.sace_home import (HomeController, HomeProvisioning, HomeInvitation,
    HomePledge, HomeAssignment, HomeDocument, HomeDocumentVersion, HomeEvidence, now)

from . import lifecycle as lc
from . import examination as ex

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
DOCUMENT_ITEMS |= {"application_form_1", "application_form_2"}


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def controller():
    row = lc.appointment()
    return db.session.get(HomeController, row.controller_id) if row else None


def require_controller():
    row = lc.require_appointment()
    return db.session.get(HomeController, row.controller_id)


def issue_provisioning(email, issued_by, days=7):
    token = secrets.token_urlsafe(32)
    row = HomeProvisioning(token_hash=digest(token), email=email.strip().lower(),
        issued_by=issued_by, expires_at=now() + timedelta(days=days))
    db.session.add(row)
    db.session.flush()
    return token


PROVISIONING_CONTEXT = 'sace_home_provisioning_context'


def start_provisioning():
    """HOME-only session bootstrap; authority is created after pledge and authentication."""
    if session.get('sace_home_pending_code'):
        abort(403, description="Continue the HOME Auditor join journey first.")
    if current_user.is_authenticated and HomeAssignment.query.filter_by(auditor_id=current_user.id).first():
        abort(403, description="HOME Auditors cannot provision controller authority.")
    nonce = secrets.token_urlsafe(32)
    session[PROVISIONING_CONTEXT] = dict(nonce=nonce, expires_at=now().timestamp() + 900,
        user_id=current_user.id if current_user.is_authenticated else None)
    session['sace_home_provisioning_token'] = nonce
    session.pop('sace_home_pledge_controller', None)
    return nonce


def provisioning(lock=False):
    token = session.get("sace_home_provisioning_token", "")
    context = session.get(PROVISIONING_CONTEXT)
    if context is not None:
        if (not isinstance(context, dict) or not token
                or not secrets.compare_digest(str(context.get('nonce', '')), token)
                or not isinstance(context.get('expires_at'), (int, float))
                or context['expires_at'] <= now().timestamp()
                or (context.get('user_id') is not None and
                    (not current_user.is_authenticated or context['user_id'] != current_user.id))):
            abort(403, description="HOME provisioning context expired or invalid. Begin a new HOME journey.")
        previous = HomeProvisioning.query.filter_by(token_hash=digest(token)).first()
        if previous and (previous.claimed_at or previous.expires_at <= now()):
            abort(403, description="This HOME provisioning context has already been used or expired.")
        return SimpleNamespace(id=token, email=current_user.email if current_user.is_authenticated else '',
            session_bootstrap=True, expires_at=context['expires_at'])
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
    lc.invitation_appointment(row)
    return row


def issue_invitation(owner):
    actor = lc.require_appointment()
    if actor.controller_id != owner.id:
        abort(403)
    code = "HOME-" + secrets.token_hex(12).upper()
    row = HomeInvitation(controller_id=owner.id, appointment_id=actor.id, code_hash=digest(code),
        expires_at=now() + timedelta(days=14))
    db.session.add(row)
    db.session.flush()
    lc.audit(current_user.id, "controller", "invitation_issued", actor.engagement_id,
        details={"invitation_id": row.id})
    return code


def consent(role, context_id):
    value = session.get("sace_home_pledge_" + role, {})
    if (value.get("context_id") != context_id or value.get("version") != PLEDGE_VERSION
            or not value.get("signature")):
        abort(400, description="Review and sign the HOME IP pledge first.")
    return value


def persist_pledge(role, row, context_id=None):
    from datetime import datetime
    value = consent(role, row.id if context_id is None else context_id)
    pledge = HomePledge(user_id=current_user.id, role=role,
        signature=(current_user.name or current_user.email)[:255]
            if value.get("acceptance_method") == "accept_and_continue"
            else value["signature"], version=PLEDGE_VERSION, text_hash=digest(PLEDGE_TEXT),
        accepted_at=datetime.fromisoformat(value["accepted_at"]),
        provisioning_id=row.id if role == "controller" else None,
        invitation_id=row.id if role == "auditor" else None)
    db.session.add(pledge)
    return pledge


def complete_provisioning():
    lc.subject_lock()
    entry = provisioning(lock=True)
    if HomeAssignment.query.filter_by(auditor_id=current_user.id).first():
        abort(403, description="HOME Auditors cannot provision controller authority.")
    context_id = entry.id
    consent('controller', context_id)
    if getattr(entry, 'session_bootstrap', False):
        from datetime import datetime, timezone
        row = HomeProvisioning.query.filter_by(token_hash=digest(entry.id)).with_for_update().first()
        if row is None:
            row = HomeProvisioning(token_hash=digest(entry.id), email=current_user.email.strip().lower(),
                issued_by='HOME session provisioning', expires_at=datetime.fromtimestamp(entry.expires_at, timezone.utc))
            db.session.add(row)
            db.session.flush()
    else:
        row = entry
    if current_user.email.strip().lower() != row.email:
        abort(403, description="Sign in with the email named in the HOME provisioning invitation.")
    pledge = persist_pledge("controller", row, context_id=context_id)
    db.session.flush()
    lc.provision(row, pledge)
    row.claimed_by, row.claimed_at = current_user.id, now()
    db.session.commit()
    session.pop("sace_home_provisioning_token", None)
    session.pop(PROVISIONING_CONTEXT, None)
    session.pop("sace_home_pledge_controller", None)


def claim():
    lc.subject_lock()
    row = invitation(lock=True)
    owner = db.session.get(HomeController, row.controller_id)
    if owner.user_id == current_user.id:
        abort(403, description="A HOME controller cannot examine their own invitation.")
    persist_pledge("auditor", row)
    assignment = HomeAssignment(invitation_id=row.id, auditor_id=current_user.id,
        requirements_version=current_app.config.get('SACE_HOME_REQUIREMENTS_VERSION', ex.REQUIREMENTS))
    db.session.add(assignment)
    row.status, row.claimed_at = "claimed", now()
    db.session.flush()
    record(assignment, "pledge", "accepted", {"version": PLEDGE_VERSION})
    record(assignment, "assignment", "claimed")
    actor = lc.invitation_appointment(row)
    lc.audit(current_user.id, "auditor", "assignment_claimed", actor.engagement_id, assignment.id,
        {"invitation_id": row.id})
    db.session.commit()
    session.pop("sace_home_pending_code", None)
    session.pop("sace_home_pledge_auditor", None)
    return assignment


def assignment(assignment_id, lock=False, writable=False):
    if lock:
        lc.subject_lock()
    row = next((item for item in lc.assignments() if item.id == assignment_id), None)
    if row is None:
        abort(403, description="An active HOME assignment and engagement are required.")
    invitation = db.session.get(HomeInvitation, row.invitation_id)
    actor = db.session.get(lc.Appointment, invitation.appointment_id)
    g.home_access = ("auditor", actor.engagement_id, row.id)
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


def document_content(version):
    from . import document_storage as storage
    try:
        return storage.read(version.storage_key, version.sha256)
    except storage.StorageUnavailable:
        abort(404, description="HOME evidence file is not available.")
    except ValueError:
        abort(409, description="HOME evidence file does not match its published version.")


def document_path(version):
    """Compatibility for disk-only internal callers; verifies the disk itself."""
    from . import document_storage as storage
    path = storage.disk_path(version.storage_key)
    try:
        storage.verify(path.read_bytes(), version.sha256)
    except OSError:
        abort(404)
    except ValueError:
        abort(409)
    return path


def examined(row, item, version=None):
    return HomeEvidence.query.filter_by(assignment_id=row.id, item=item,
        event="examined", document_version_id=version).first() is not None


def board_items(row):
    if row.requirements_version == ex.REQUIREMENTS:
        return ex.board_items(row)
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


def publish_document(owner, kind, version_label, storage_key, manifest, *, content=None):
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
    from . import document_storage as storage
    storage.validate_key(storage_key)
    expected = manifest.get("pdf_sha256")
    if content is None:
        content = storage.staged(storage_key, expected)
    content_hash = hashlib.sha256(content).hexdigest()
    storage.verify(content, expected or content_hash)
    if hashlib.sha256(content).hexdigest() in ex.reading_artifact_hashes():
        raise ValueError('Frozen Reading documents cannot be published as HOME evidence.')
    if kind in {'application_form_1', 'application_form_2'}:
        approval = manifest.get('home_approval', {})
        if (manifest.get('subject') != SUBJECT or manifest.get('kind') != kind
                or not approval.get('approved_by') or not approval.get('reference')):
            raise ValueError('Application forms require explicit HOME provenance and approval.')
    # Serialise publication so simultaneous calls cannot create the same document kind.
    from app.models.auth import User
    actor = lc.appointment(db.session.get(User, owner.user_id), lock=True)
    if actor is None or actor.controller_id != owner.id:
        raise ValueError("An active HOME controller appointment is required to publish evidence.")
    db.session.execute(db.text("SELECT pg_advisory_xact_lock(74831029)"))
    document = HomeDocument.query.filter_by(kind=kind).first()
    if document is None:
        document = HomeDocument(kind=kind, title=ex.ITEMS.get(kind, ITEMS.get(kind)))
        db.session.add(document)
        db.session.flush()
    if HomeDocumentVersion.query.filter_by(document_id=document.id, version=version_label).first():
        raise ValueError("That HOME document version already exists; publish a new version instead.")
    storage.store(storage_key, content, expected or content_hash)
    row = HomeDocumentVersion(document_id=document.id, version=version_label,
        storage_key=storage_key, sha256=content_hash,
        source_manifest=manifest, approved_by=owner.id)
    db.session.add(row)
    db.session.flush()
    return row
