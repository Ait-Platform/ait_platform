"""One shared Proposal, authorized by existing current elected appointments."""
from datetime import datetime, timezone
from flask import abort
from sqlalchemy import func, or_
from app.extensions import db
from app.models.auth import User
from app.models.uip import UipProposal, UipProposalDocument, UipResolution, UipDocument, UipCommitteeMeeting
from app.models.uip_governance import UipCommitteeMember
from . import documents
from .finance import money
from .reception import text, identifier

POSITIONS = ("secretary", "chairperson", "vice-chairperson", "treasurer")


def appointment(org, actor):
    user = db.session.get(User, actor)
    if not user or not user.is_active or not (user.email or "").strip():
        return None
    return UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org,
        UipCommitteeMember.status == "CURRENT",
        func.lower(func.trim(UipCommitteeMember.email)) == user.email.strip().lower(),
        func.lower(func.trim(UipCommitteeMember.position)).in_(POSITIONS)
    ).order_by(UipCommitteeMember.id).first()


def require_official(org, actor):
    row = appointment(org, actor)
    if not row:
        abort(403)
    return row


def listing(org, actor):
    require_official(org, actor)
    return UipProposal.query.filter(UipProposal.organization_id == org,
        or_(UipProposal.originator_id == actor, UipProposal.status != "DRAFT")
    ).order_by(UipProposal.id.desc()).all()


def get(org, actor, proposal_id, lock=False):
    require_official(org, actor)
    query = UipProposal.query.filter_by(organization_id=org, id=proposal_id)
    if lock:
        query = query.populate_existing().with_for_update()
    row = query.first_or_404()
    if row.status == "DRAFT" and row.originator_id != actor:
        abort(404)
    return row


def available_documents(org, actor):
    return [doc for doc in UipDocument.query.filter_by(organization_id=org).order_by(UipDocument.id.desc()).all()
            if documents.accessible(org, actor, doc)]


def save(org, actor, values, document_ids, proposal_id=None):
    authority = require_official(org, actor)
    row = get(org, actor, proposal_id, lock=True) if proposal_id else UipProposal(
        organization_id=org, originator_id=actor, originating_capacity=authority.position, status="DRAFT")
    if row.status != "DRAFT" or row.originator_id != actor:
        abort(409, description="Only the originator can edit a working draft.")
    sub_id = values.get("originating_subcommittee_id")
    if sub_id:
        from .subcommittees import require_subcommittee_responsibility
        sub = require_subcommittee_responsibility(org, actor, sub_id)
        row.originating_subcommittee_id = sub.id
    else:
        row.originating_subcommittee_id = None
    # Validate everything before mutating the draft or its document links.
    title = text(values.get("title"), 255, True)
    description = text(values.get("description"), 20000, True)
    motivation = text(values.get("motivation"), 20000, True)
    amount = values.get("proposed_budget", "")
    budget = money(amount, zero=True) if amount is not None and str(amount).strip() else None
    selected = []
    for doc_id in set(document_ids):
        doc = UipDocument.query.filter_by(organization_id=org, id=identifier(doc_id)).first_or_404()
        if not documents.accessible(org, actor, doc):
            abort(404)
        selected.append(doc)
    row.title, row.description, row.motivation, row.proposed_budget = title, description, motivation, budget
    db.session.add(row)
    db.session.flush()
    UipProposalDocument.query.filter_by(proposal_id=row.id, organization_id=org).delete(synchronize_session=False)
    for doc in selected:
        db.session.add(UipProposalDocument(proposal_id=row.id, organization_id=org, document_id=doc.id))
    return row


def submit(org, actor, proposal_id):
    row = get(org, actor, proposal_id, lock=True)
    if row.originator_id != actor:
        abort(403)
    if row.status == "DRAFT":
        row.status = "SUBMITTED"
        row.submitted_at = datetime.now(timezone.utc)
    return row


def convert(org, actor, proposal_id, meeting_id):
    # Same four elected positions already qualify for formal draft creation.
    row = get(org, actor, proposal_id, lock=True)
    if row.status == "CONVERTED":
        return row
    if row.status != "SUBMITTED":
        abort(409, description="Submit the Proposal before creating a Resolution draft.")
    meeting = UipCommitteeMeeting.query.filter_by(organization_id=org, id=identifier(meeting_id)).first_or_404()
    resolution = UipResolution(organization_id=org, meeting_id=meeting.id,
        title=row.title, description=f"Proposal {row.reference}\n\n{row.description}\n\nMotivation\n{row.motivation}",
        status="DRAFT", voting_scope="EXCO", quorum_target=50, recorded_by=actor,
        result_basis={"type": "proposal", "proposal_id": row.id, "proposal_reference": row.reference})
    db.session.add(resolution)
    db.session.flush()
    row.resolution_id = resolution.id
    row.status = "CONVERTED"
    row.converted_by = actor
    row.converted_at = datetime.now(timezone.utc)
    return row
