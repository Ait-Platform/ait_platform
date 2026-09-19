import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_block = """    if decision == "ADOPTED":
        votes = res.votes.all() if hasattr(res, 'votes') else []
        quorum_target = getattr(res, 'quorum_target', 50)
        scope = getattr(res, 'voting_scope', 'EXCO')
        
        total_eligible = 0
        if scope in ['EXCO', 'EXCO_CORE', 'COMMITTEE_ALL', 'SUB_COMMITTEE']:
            total_eligible = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").count()
        else:
            from app.models.core import CoreOrganizationMember
            total_eligible = CoreOrganizationMember.query.filter_by(organization_id=org.id, is_active=True).count()
            
        if total_eligible == 0:
            total_eligible = 1
            
        current_quorum_pct = int((len(votes) / total_eligible) * 100)
        
        if current_quorum_pct < quorum_target:
            flash(f"Digital quorum was not met ({current_quorum_pct}% of {quorum_target}%). Proceeding anyway, as final ratification occurs at the live meeting.", "warning")"""

new_block = """    if decision == "ADOPTED":
        votes = res.votes.all() if hasattr(res, 'votes') else []
        scope = getattr(res, 'voting_scope', 'EXCO')
        
        if scope in ['EXCO', 'EXCO_CORE', 'COMMITTEE_ALL', 'SUB_COMMITTEE']:
            # Get all eligible committee members
            if scope == 'EXCO_CORE':
                eligible_members = UipCommitteeMember.query.filter(
                    UipCommitteeMember.organization_id == org.id, 
                    UipCommitteeMember.status == "CURRENT",
                    UipCommitteeMember.position.in_(["Chairperson", "Vice-Chairperson", "Secretary", "Treasurer"])
                ).all()
            else:
                eligible_members = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").all()
            
            voted_user_ids = [v.user_id for v in votes]
            missing_members = [m.name for m in eligible_members if m.user_id not in voted_user_ids]
            
            if missing_members:
                missing_names = ", ".join(missing_members)
                flash(f"Resolution proceeded. WARNING: The following elected members violated the mandatory voting rule by failing to cast a digital vote: {missing_names}", "warning")
            else:
                flash("Resolution proceeded. All elected members successfully cast their mandatory digital votes.", "success")
                
        else:
            # For PUBLIC/Ratepayer scopes, we don't name-shame 400 people
            from app.models.core import CoreOrganizationMember
            total_eligible = CoreOrganizationMember.query.filter_by(organization_id=org.id, is_active=True).count()
            if total_eligible == 0: total_eligible = 1
            current_quorum_pct = int((len(votes) / total_eligible) * 100)
            flash(f"Public vote reached {current_quorum_pct}% participation. Proceeding to live ratification.", "info")"""

text = text.replace(old_block, new_block)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated quorum logic to name-shame missing committee voters")
