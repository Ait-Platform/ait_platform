import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

helper = """
def execute_resolution_adoption(org, res, db):
    res.status = "ADOPTED"
    if res.result_basis and res.result_basis.get("type") == "access_bundle":
        from app.models.core import CoreInteraction, CoreOrganizationMember, CoreRoleAssignment, CoreRole
        from app.models.uip_governance import UipCommitteeMember
        
        interaction_ids = res.result_basis.get("interaction_ids", [])
        portfolio_map = res.result_basis.get("portfolios", {})
        
        claims = CoreInteraction.query.filter(CoreInteraction.id.in_(interaction_ids)).all()
        for claim in claims:
            claim.status = "VERIFIED"
            
            # Smart Merge: Look for a Ghost Row (Vault Upload without user_id)
            from app.models.uip import UipMemberProfile
            profile = UipMemberProfile.query.filter(UipMemberProfile.email.ilike(claim.creator.email)).first()
            if profile and profile.organization_member_id:
                org_mem = CoreOrganizationMember.query.get(profile.organization_member_id)
                if org_mem and not org_mem.user_id:
                    org_mem.user_id = claim.creator.id
                    org_mem.is_active = True
            
            # Ensure they have an active organization membership
            org_mem = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=claim.creator.id).first()
            if not org_mem:
                org_mem = CoreOrganizationMember(organization_id=org.id, user_id=claim.creator.id, is_active=True)
                db.session.add(org_mem)
            else:
                org_mem.is_active = True
                
            # Grant the role
            role_slug = "committee_member" if "committee" in claim.interaction_type or "secretary" in claim.interaction_type else "mo" if "mo" in claim.interaction_type else "ratepayer"
            role_obj = CoreRole.query.filter_by(slug=role_slug).first()
            if role_obj:
                existing_role = CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id).first()
                if not existing_role:
                    db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id))

            # If committee, make them an official member
            if 'committee' in claim.interaction_type or 'secretary' in claim.interaction_type:
                port = portfolio_map.get(str(claim.id)) or portfolio_map.get(claim.id) or ("Secretary" if "secretary" in claim.interaction_type else claim.interaction_type.replace('_claim', '').title())
                mem = UipCommitteeMember(
                    organization_id=org.id,
                    name=claim.creator.name,
                    email=claim.creator.email,
                    position=port,
                    status="CURRENT"
                )
                db.session.add(mem)
"""

# Replace the giant adoption block in decide_resolution
old_decide_block = """    res.status = decision
    
    # 2. If ADOPTED and it is an Access Bundle, grant the roles
    if decision == "ADOPTED" and res.result_basis and res.result_basis.get("type") == "access_bundle":
        from app.models.core import CoreInteraction, CoreOrganizationMember, CoreRoleAssignment, CoreRole
        
        interaction_ids = res.result_basis.get("interaction_ids", [])
        portfolio_map = res.result_basis.get("portfolios", {})
        
        claims = CoreInteraction.query.filter(CoreInteraction.id.in_(interaction_ids)).all()
        for claim in claims:
            claim.status = "VERIFIED"
            
            # Smart Merge: Look for a Ghost Row (Vault Upload without user_id)
            from app.models.uip import UipMemberProfile
            profile = UipMemberProfile.query.filter(UipMemberProfile.email.ilike(claim.creator.email)).first()
            if profile and profile.organization_member_id:
                org_mem = CoreOrganizationMember.query.get(profile.organization_member_id)
                if org_mem and not org_mem.user_id:
                    org_mem.user_id = claim.creator.id
                    org_mem.is_active = True
            
            # Ensure they have an active organization membership
            org_mem = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=claim.creator.id).first()
            if not org_mem:
                org_mem = CoreOrganizationMember(organization_id=org.id, user_id=claim.creator.id, is_active=True)
                db.session.add(org_mem)
            else:
                org_mem.is_active = True
                
            # Grant the role
            role_slug = "committee_member" if "committee" in claim.interaction_type or "secretary" in claim.interaction_type else "mo" if "mo" in claim.interaction_type else "ratepayer"
            role_obj = CoreRole.query.filter_by(slug=role_slug).first()
            if role_obj:
                existing_role = CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id).first()
                if not existing_role:
                    db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id))

            # If committee, make them an official member
            if 'committee' in claim.interaction_type or 'secretary' in claim.interaction_type:
                port = portfolio_map.get(str(claim.id)) or portfolio_map.get(claim.id) or ("Secretary" if "secretary" in claim.interaction_type else claim.interaction_type.replace('_claim', '').title())
                mem = UipCommitteeMember(
                    organization_id=org.id,
                    name=claim.creator.name,
                    email=claim.creator.email,
                    position=port,
                    status="CURRENT"
                )
                db.session.add(mem)
                
    db.session.commit()"""

new_decide_block = """    if decision == "ADOPTED":
        execute_resolution_adoption(org, res, db)
    else:
        res.status = decision
    db.session.commit()"""

# Replace in text
text = helper + text
text = text.replace(old_decide_block, new_decide_block)

# Now, add auto-close to vote_resolution
old_vote_end = """        db.session.add(new_vote)
        flash("Your secure vote has been cast.", "success")
        
    db.session.commit()
    return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))"""

new_vote_end = """        db.session.add(new_vote)
        flash("Your secure vote has been cast.", "success")
        
    db.session.commit()
    
    # --- AUTO-CLOSE LOGIC ---
    votes = res.votes.all() if hasattr(res, 'votes') else []
    total_eligible = 0
    if scope in ['EXCO', 'EXCO_CORE', 'COMMITTEE_ALL', 'SUB_COMMITTEE']:
        if scope == 'EXCO_CORE':
            total_eligible = UipCommitteeMember.query.filter(
                UipCommitteeMember.organization_id == org.id, 
                UipCommitteeMember.status == "CURRENT",
                UipCommitteeMember.position.in_(["Chairperson", "Vice-Chairperson", "Secretary", "Treasurer"])
            ).count()
        else:
            total_eligible = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").count()
    else:
        from app.models.core import CoreOrganizationMember
        total_eligible = CoreOrganizationMember.query.filter_by(organization_id=org.id, is_active=True).count()
        
    if total_eligible > 0 and len(votes) >= total_eligible:
        yea_count = len([v for v in votes if v.vote == 'YEA'])
        nay_count = len([v for v in votes if v.vote == 'NAY'])
        
        if yea_count > nay_count:
            execute_resolution_adoption(org, res, db)
            db.session.commit()
            flash(f"Voting concluded automatically! 100% participation reached. Resolution ADOPTED.", "success")
        else:
            res.status = "REJECTED"
            db.session.commit()
            flash(f"Voting concluded automatically! 100% participation reached. Resolution REJECTED.", "warning")
            
    return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))"""

text = text.replace(old_vote_end, new_vote_end)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Implemented auto-close logic")
