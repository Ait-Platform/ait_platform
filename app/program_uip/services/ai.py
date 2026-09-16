"""UIP permission/context adapter. No direct model APIs or operational mutations."""
import json
import re
from flask import abort
from app.extensions import db
from app.models.core import CoreInteraction
from app.models.uip import UipAuditEvent
from app.services import ait_ai_gateway as gateway
from . import audit
ROLES=("manager","committee_member","receptionist")
TASKS={"communication":"Draft a professional communication", "governance_notice":"Draft a governance notice", "minutes":"Draft meeting/minutes wording", "issue":"Summarise an issue", "category":"Suggest an issue category", "activity":"Summarise operational activity", "finance":"Explain recorded Finance figures"}


def event(org,actor,action,row):
    db.session.add(UipAuditEvent(organization_id=org,actor_user_id=actor,action=action,entity_type=type(row).__name__,entity_id=row.id,metadata_json={}))


def safe_text(value):
    value=(value or "").strip()
    if len(value)>4000: abort(400,description="Keep approved context under 4,000 characters.")
    # Reject common secret/bank patterns; context is explicitly reviewed by the operator.
    if re.search(r"(?i)(password|api[ _-]?key|bearer |bank account|account number|iban|-----BEGIN|sk-[a-zA-Z0-9]{10})",value):
        abort(400,description="Remove credentials and banking details from AI context.")
    value=re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}","[email omitted]",value)
    value=re.sub(r"(?<!\w)\+?[\d ()-]{9,}(?!\w)","[number omitted]",value)
    return value


def run(org,actor,feature,context,key,issue_id=None):
    audit.authorize(org,actor,ROLES)
    if feature not in TASKS: abort(400)
    approved=safe_text(context)
    if feature in {"issue","category"}:
        audit.authorize(org,actor,("manager","receptionist"))
        # Only minimal structured issue metadata; no private descriptions or documents.
        issue=CoreInteraction.query.filter_by(organization_id=org,id=issue_id).first_or_404()
        approved=json.dumps(dict(category=issue.category,status=issue.status,priority=issue.priority))+"\nApproved source text: "+approved
    elif feature=="activity":
        audit.authorize(org,actor,("manager","receptionist"))
        counts=db.session.query(CoreInteraction.status,db.func.count()).filter(CoreInteraction.organization_id==org).group_by(CoreInteraction.status).all()
        approved="Current recorded issue counts: "+json.dumps(dict(counts))+"\n"+approved
    elif feature=="finance":
        audit.authorize(org,actor,("manager",))
        from .finance import overview
        figures=overview(org,actor)
        approved="Recorded values, not bank verified: "+json.dumps({k:str(figures[k]) for k in ("label","cash","outstanding","available","income","expenditure")})+"\n"+approved
    elif not approved: abort(400,description="Supply a short, approved description for the draft.")
    return gateway.ask(org,actor,"uip",feature,TASKS[feature]+". Advisory draft only.\n"+approved,key,
        lambda action,row:event(org,actor,action,row))
