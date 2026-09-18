import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Fix draft_resolution auth
old_draft_auth = """    sec_check = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        UipCommitteeMember.position == "Secretary",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not sec_check:
        flash("Only the Secretary can draft resolutions.", "error")"""

new_draft_auth = """    exco_check = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not exco_check:
        flash("Only active Committee Members can draft resolutions.", "error")"""
text = text.replace(old_draft_auth, new_draft_auth)

# Fix publish_resolution auth
old_pub_auth = """    sec_check = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        UipCommitteeMember.position == "Secretary",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not sec_check:
        flash("Only the Secretary can publish resolutions.", "error")"""

new_pub_auth = """    exco_check = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not exco_check:
        flash("Only active Committee Members can publish resolutions.", "error")"""
text = text.replace(old_pub_auth, new_pub_auth)

# Fix query in committee_dashboard to allow all exco members to see drafts (Wait, actually if they can all draft, they should all see DRAFTs!)
old_query = """    is_secretary = current_appointment and current_appointment.position == 'Secretary'
    if is_secretary:
        all_resolutions = UipResolution.query.filter_by(organization_id=org.id).order_by(UipResolution.created_at.desc()).all()
    else:
        all_resolutions = UipResolution.query.filter(
            UipResolution.organization_id == org.id,
            UipResolution.status != 'DRAFT'
        ).order_by(UipResolution.created_at.desc()).all()"""

new_query = """    all_resolutions = UipResolution.query.filter_by(organization_id=org.id).order_by(UipResolution.created_at.desc()).all()"""
text = text.replace(old_query, new_query)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated committee_routes.py auth checks")
