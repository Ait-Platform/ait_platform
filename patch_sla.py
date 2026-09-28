import re

filepath = 'app/program_uip/services/sla.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace acknowledge
old_ack = '''def acknowledge(org, actor, issue_id):
    audit.authorize(org, actor, providers.STAFF)
    issue = operations.issue(org, issue_id)
    operations.open_issue(issue)
    # An audit event records the actual action even when no policy exists.
    from app.models.uip import UipAuditEvent
    if UipAuditEvent.query.filter_by(organization_id=org, entity_type="CoreInteraction",
                                    entity_id=issue.id, action="interaction.acknowledged").first():
        abort(409, description="Acknowledgement is already recorded.")
    finish(issue, "acknowledgement", datetime.now(timezone.utc))
    audit.record(org, actor, "interaction.acknowledged", issue)'''

new_ack = '''def acknowledge(org, actor, issue_id):
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func, or_
    from flask_login import current_user
    try:
        audit.authorize(org, actor, providers.STAFF)
    except Exception:
        email_check = False
        if current_user.email and current_user.email.strip():
            email_check = func.lower(func.trim(UipCommitteeMember.email)) == current_user.email.strip().lower()
        is_committee = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org,
            UipCommitteeMember.status == "CURRENT",
            or_(UipCommitteeMember.user_id == actor, email_check)
        ).first()
        if not is_committee:
            raise

    issue = operations.issue(org, issue_id)
    operations.open_issue(issue)
    # An audit event records the actual action even when no policy exists.
    from app.models.uip import UipAuditEvent
    if UipAuditEvent.query.filter_by(organization_id=org, entity_type="CoreInteraction",
                                    entity_id=issue.id, action="interaction.acknowledged").first():
        abort(409, description="Acknowledgement is already recorded.")
        
    finish(issue, "acknowledgement", datetime.now(timezone.utc))
    audit.record(org, actor, "interaction.acknowledged", issue)
    
    # Auto-Acknowledge Children (Task 7)
    from app.models.core import CoreInteraction
    from app.models.uip import UipCommunicationLog
    children = CoreInteraction.query.filter_by(organization_id=org, parent_id=issue.id).all()
    for child in children:
        if not UipAuditEvent.query.filter_by(organization_id=org, entity_type="CoreInteraction", entity_id=child.id, action="interaction.acknowledged").first():
            finish(child, "acknowledgement", datetime.now(timezone.utc))
            audit.record(org, actor, "interaction.acknowledged", child)
            comm = UipCommunicationLog(
                organization_id=org,
                interaction_id=child.id,
                channel="EMAIL",
                party_classification="MEMBER",
                purpose="ACKNOWLEDGEMENT",
                status="RECORDED",
                summary="[AUTO-REPLY] Merged query acknowledged via Master Ticket #" + str(issue.id)
            )
            db.session.add(comm)'''

content = content.replace(old_ack, new_ack)

# Replace overview
old_overview = '''def overview(org, actor):
    audit.authorize(org, actor, providers.STAFF)'''
new_overview = '''def overview(org, actor):
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func, or_
    from flask_login import current_user
    try:
        audit.authorize(org, actor, providers.STAFF)
    except Exception:
        email_check = False
        if current_user.email and current_user.email.strip():
            email_check = func.lower(func.trim(UipCommitteeMember.email)) == current_user.email.strip().lower()
        is_committee = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org,
            UipCommitteeMember.status == "CURRENT",
            or_(UipCommitteeMember.user_id == actor, email_check)
        ).first()
        if not is_committee:
            raise'''

content = content.replace(old_overview, new_overview)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated sla.py")
