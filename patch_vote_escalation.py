import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Update total_eligible in view_resolution
old_eligibility_view = """    total_eligible = 0
    if scope == 'EXCO':
        total_eligible = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").count()
    else:
        from app.models.core import CoreOrganizationMember
        total_eligible = CoreOrganizationMember.query.filter_by(organization_id=org.id, is_active=True).count()"""

new_eligibility_view = """    total_eligible = 0
    if scope == 'EXCO_CORE':
        total_eligible = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org.id, 
            UipCommitteeMember.status == "CURRENT",
            UipCommitteeMember.position.in_(["Chairperson", "Vice-Chairperson", "Secretary", "Treasurer"])
        ).count()
    elif scope in ['EXCO', 'COMMITTEE_ALL']:
        total_eligible = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").count()
    else:
        from app.models.core import CoreOrganizationMember
        total_eligible = CoreOrganizationMember.query.filter_by(organization_id=org.id, is_active=True).count()"""

text = text.replace(old_eligibility_view, new_eligibility_view)

# Update eligibility in vote_resolution
old_vote_eligibility = """    # Verify Eligibility
    if scope == 'EXCO':
        appointment = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org.id,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
        ).first()
        if not appointment:
            abort(403)"""

new_vote_eligibility = """    # Verify Eligibility
    appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if scope == 'EXCO_CORE':
        if not appointment or appointment.position not in ["Chairperson", "Vice-Chairperson", "Secretary", "Treasurer"]:
            abort(403)
    elif scope in ['EXCO', 'COMMITTEE_ALL']:
        if not appointment:
            abort(403)"""
text = text.replace(old_vote_eligibility, new_vote_eligibility)

# Update the vote processing logic to escalate on NAY
old_vote_process = """    existing = UipResolutionVote.query.filter_by(resolution_id=res.id, user_id=current_user.id).first()
    if existing:
        existing.vote = vote_val
    else:
        v = UipResolutionVote(resolution_id=res.id, user_id=current_user.id, vote=vote_val)
        db.session.add(v)
        
    db.session.commit()
    flash("Your vote was securely recorded.", "success")"""

new_vote_process = """    existing = UipResolutionVote.query.filter_by(resolution_id=res.id, user_id=current_user.id).first()
    if existing:
        existing.vote = vote_val
    else:
        v = UipResolutionVote(resolution_id=res.id, user_id=current_user.id, vote=vote_val)
        db.session.add(v)
        
    # The 1-Nay Escalation Rule
    if vote_val == "NAY" and scope in ['EXCO_CORE', 'EXCO', 'COMMITTEE_ALL']:
        res.status = "ESCALATED"
        flash("You logged a dissenting vote. Digital voting has been automatically suspended and this resolution has been escalated to a live meeting.", "warning")
    else:
        flash("Your vote was securely recorded.", "success")
        
    db.session.commit()"""
text = text.replace(old_vote_process, new_vote_process)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated scopes and dissent escalation in committee_routes.py")
