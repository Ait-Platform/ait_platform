"""Bounded, explicit Flask-Mail invitations; only digests persist."""
import hashlib
import secrets
import re
from datetime import datetime, timezone, timedelta
from urllib.parse import urlsplit
from flask import abort, current_app
from flask_mail import Message
from app.extensions import db, mail
from app.models.uip import UipMemberProfile, UipAuditEvent
from app.models.uip_governance import UipSurvey, UipSurveyResponse
from app.models.uip_invitations import UipVotingInvitation as Invitation
from . import governance, audit
from .sla import utc


def event(org, actor, action, survey):
    audit.authorize(org, actor, governance.ADMIN)
    db.session.add(UipAuditEvent(organization_id=org, actor_user_id=actor, action=action,
        entity_type="UipSurvey", entity_id=survey.id, metadata_json={}))


def email_address(member):
    value = (member.email or "").strip()
    return value if len(value) <= 254 and re.fullmatch(r"[^\s@<>,;]+@[^\s@<>,;]+\.[^\s@<>,;]+", value) else None


def roster(org, actor, survey_id):
    audit.authorize(org, actor, governance.ADMIN)
    row = UipSurvey.query.filter_by(organization_id=org, id=survey_id).first_or_404()
    basis = row.eligibility_snapshot if row.eligibility_snapshot is not None else governance.eligibility(org, datetime.now(timezone.utc).date(), row.relationship)
    votes = {v.member_id for v in UipSurveyResponse.query.filter_by(organization_id=org, survey_id=row.id).all()}
    invites = {i.member_id:i for i in Invitation.query.filter_by(organization_id=org, survey_id=row.id).all()}
    members = UipMemberProfile.query.filter(UipMemberProfile.organization_id==org, UipMemberProfile.id.in_([int(k) for k in basis])).order_by(UipMemberProfile.id).all()
    return row, [dict(member=m, email=email_address(m), voted=m.id in votes, invitation=invites.get(m.id)) for m in members]


def plan(rows, member_id=None):
    return [dict(member=r["member"].id, address_digest=hashlib.sha256(r["email"].encode()).hexdigest())
        for r in rows if r["email"] and not r["voted"] and
        (r["member"].id==member_id if member_id else not r["invitation"] or r["invitation"].status=="FAILED")][:100]


def send(org, actor, survey_id, selected):
    """Owns delivery transactions: attempted evidence is durable before SMTP."""
    audit.authorize(org.id, actor, governance.ADMIN)
    base = current_app.config.get("UIP_PUBLIC_BASE_URL", "")
    parsed = urlsplit(base)
    if not parsed.netloc or parsed.scheme != "https" or parsed.query or parsed.fragment or parsed.username:
        abort(503, description="Voting email is unavailable: configure the trusted HTTPS UIP public URL.")
    if not current_app.config.get("MAIL_DEFAULT_SENDER") or "mail" not in current_app.extensions:
        abort(503, description="Voting email is unavailable: configure the application mail system.")
    mail_state=current_app.extensions["mail"]
    if mail_state.debug or (current_app.testing and not mail_state.suppress):
        abort(503, description="Disable mail debug output and use suppressed mail for tests.")
    row = UipSurvey.query.filter_by(organization_id=org.id,id=survey_id).populate_existing().with_for_update().first_or_404()
    if row.status != "OPEN" or datetime.now(timezone.utc)>=utc(row.closes_at):
        abort(409, description="This survey has closed.")
    if row.eligibility_snapshot is None:
        row.eligibility_snapshot=governance.eligibility(org.id,datetime.now(timezone.utc).date(),row.relationship)
    event(org.id,actor,"vote_invitation.batch_initiated",row)
    db.session.commit()
    for choice in selected:
        row = UipSurvey.query.filter_by(organization_id=org.id,id=survey_id).populate_existing().with_for_update().one()
        if row.status!="OPEN" or datetime.now(timezone.utc)>=utc(row.closes_at): break
        member=UipMemberProfile.query.filter_by(organization_id=org.id,id=choice["member"]).first()
        if not member or str(member.id) not in row.eligibility_snapshot: continue
        address=email_address(member)
        if not address or hashlib.sha256(address.encode()).hexdigest()!=choice["address_digest"]: continue
        if UipSurveyResponse.query.filter_by(organization_id=org.id,survey_id=row.id,member_id=member.id).first(): continue
        invite=Invitation.query.filter_by(organization_id=org.id,survey_id=row.id,member_id=member.id).first()
        now=datetime.now(timezone.utc)
        if invite and invite.status=="ATTEMPTED" and utc(invite.attempted_at)>now-timedelta(minutes=5): continue
        token=secrets.token_urlsafe(32)
        if invite:
            event(org.id,actor,"vote_invitation.resent",row)
        else:
            invite=Invitation(organization_id=org.id,survey_id=row.id,member_id=member.id,attempts=0)
            db.session.add(invite)
        invite.token_digest=hashlib.sha256(token.encode()).hexdigest()
        invite.expires_at=utc(row.closes_at)
        invite.attempts+=1
        invite.status,invite.attempted_at,invite.accepted_at,invite.error_category="ATTEMPTED",now,None,None
        event(org.id,actor,"vote_invitation.attempted",row)
        # Token in fragment never reaches web access logs or Referer headers.
        link=base.rstrip("/")+f"/uip/vote/{org.slug}/{row.id}#"+token
        body=f"{org.name} - UIP voting invitation\n\nDear {member.name},\n\n{row.title}\n{row.purpose}\n\nCloses: {utc(row.closes_at).isoformat()} (UTC)\n\nVote Now: {link}\n\nThis private link is intended only for the named eligible recipient. Do not forward it. It permits one response to this survey only."
        iid,digest=invite.id,invite.token_digest
        db.session.flush(); iid=invite.id
        db.session.commit()
        state,error="ACCEPTED",None
        try:
            mail.send(Message(subject=f"{org.name}: voting invitation",recipients=[address],body=body))
            if mail_state.suppress: state="TEST_SUPPRESSED"
        except Exception:
            state,error="FAILED","mail_error"
        invite=Invitation.query.filter_by(id=iid,token_digest=digest).with_for_update().first()
        if invite:
            invite.status,invite.error_category=state,error
            invite.accepted_at=datetime.now(timezone.utc) if state=="ACCEPTED" else None
            event(org.id,actor,"vote_invitation.failed" if error else "vote_invitation."+state.lower(),row)
        db.session.commit()


def entitlement(org, survey_id, token):
    if not isinstance(token,str) or not re.fullmatch(r"[A-Za-z0-9_-]{43}",token): abort(404)
    row=UipSurvey.query.filter_by(organization_id=org,id=survey_id).populate_existing().with_for_update().first_or_404()
    invite=Invitation.query.filter_by(organization_id=org,survey_id=survey_id,token_digest=hashlib.sha256(token.encode()).hexdigest()).first_or_404()
    if datetime.now(timezone.utc)>=utc(invite.expires_at): abort(410,description="This voting invitation has expired.")
    basis=governance.response_basis(row,invite.member_id)
    if UipSurveyResponse.query.filter_by(organization_id=org,survey_id=survey_id,member_id=invite.member_id).first():
        abort(409,description="Your response has already been recorded. Thank you.")
    return row,invite,basis


def vote(org,survey_id,token,answers):
    row,invite,basis=entitlement(org,survey_id,token)
    return governance.store_response(row,None,invite.member_id,answers,basis)
