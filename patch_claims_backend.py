import re
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_portfolio_logic = """    # Process portfolio assignments
    portfolio_map = {}
    for claim in claims:
        port = request.form.get(f"portfolio_{claim.id}", "").strip()
        if port:
            portfolio_map[claim.id] = port
            
    from datetime import datetime
    current_year = datetime.now().year
    
    if target == "founding":
        # Find the founding resolution
        founding_res = UipResolution.query.filter_by(organization_id=org.id).filter(UipResolution.title.ilike("%Founding%")).first()
        if founding_res:
            additions = "\n\n-- Added via Inaugural Roster --\n"
            for claim in claims:
                port = portfolio_map.get(claim.id, claim.interaction_type.replace('_claim', '').title())
                additions += f"- {claim.creator.name} ({claim.creator.email}) as {port}\n"
                claim.status = "VERIFIED"
                
                from app.models.core import CoreOrganizationMember, CoreRoleAssignment, CoreRole
                
                # Ensure they have an active organization membership
                org_mem = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=claim.creator.id).first()
                if not org_mem:
                    org_mem = CoreOrganizationMember(organization_id=org.id, user_id=claim.creator.id, is_active=True)
                    db.session.add(org_mem)
                else:
                    org_mem.is_active = True
                    
                # Grant the appropriate role
                role_slug = "committee_member" if "committee" in claim.interaction_type else "mo" if "mo" in claim.interaction_type else "ratepayer"
                role_obj = CoreRole.query.filter_by(slug=role_slug).first()
                if role_obj:
                    # check if they have it
                    existing_role = CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id).first()
                    if not existing_role:
                        db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id))
                        
                if 'committee' in claim.interaction_type:
                    from app.models.uip_governance import UipCommitteeMember
                    mem = UipCommitteeMember(
                        organization_id=org.id,
                        user_id=claim.creator.id,
                        name=claim.creator.name,
                        email=claim.creator.email,
                        position=port,
                        status="CURRENT"
                    )
                    db.session.add(mem)
                    
            founding_res.description += additions
            db.session.commit()
            flash("Members successfully officially logged into the Founding Resolution!", "success")
            return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
            
    # Fallback or "new" resolution logic
    for claim in claims:
        claim.status = "PENDING_RESOLUTION"
        
    res_count = UipResolution.query.filter_by(organization_id=org.id).count() + 1
    
    res = UipResolution(
        organization_id=org.id,
        title=f"Resolution {current_year}-{res_count} - Access Bundle",
        description="Resolution to grant active platform access to the bundled applicants.\n",
        status="PROPOSED",
        recorded_by=current_user.id,
        result_basis={"type": "access_bundle", "interaction_ids": [c.id for c in claims], "portfolios": portfolio_map}
    )
    for claim in claims:
        port = portfolio_map.get(claim.id, claim.interaction_type.replace('_claim', '').title())
        res.description += f"\\n- {claim.creator.name}: {port}"
        
    db.session.add(res)"""

new_portfolio_logic = """    from datetime import datetime
    current_year = datetime.now().year
    
    if target == "founding":
        # Find the founding resolution
        founding_res = UipResolution.query.filter_by(organization_id=org.id).filter(UipResolution.title.ilike("%Founding%")).first()
        if founding_res:
            additions = "\\n\\n-- Added via Inaugural Roster --\\n"
            for claim in claims:
                role_name = claim.interaction_type.replace('_claim', '').title()
                additions += f"- {claim.creator.name} ({claim.creator.email}) as {role_name}\\n"
                claim.status = "VERIFIED"
                
                from app.models.core import CoreOrganizationMember, CoreRoleAssignment, CoreRole
                
                # Ensure they have an active organization membership
                org_mem = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=claim.creator.id).first()
                if not org_mem:
                    org_mem = CoreOrganizationMember(organization_id=org.id, user_id=claim.creator.id, is_active=True)
                    db.session.add(org_mem)
                else:
                    org_mem.is_active = True
                    
                # Grant the appropriate role
                role_slug = "committee_member" if "committee" in claim.interaction_type else "mo" if "mo" in claim.interaction_type else "ratepayer"
                role_obj = CoreRole.query.filter_by(slug=role_slug).first()
                if role_obj:
                    # check if they have it
                    existing_role = CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id).first()
                    if not existing_role:
                        db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id))
                        
                if 'committee' in claim.interaction_type:
                    from app.models.uip_governance import UipCommitteeMember
                    mem = UipCommitteeMember(
                        organization_id=org.id,
                        user_id=claim.creator.id,
                        name=claim.creator.name,
                        email=claim.creator.email,
                        position="Unassigned",
                        status="CURRENT"
                    )
                    db.session.add(mem)
                    
            founding_res.description += additions
            db.session.commit()
            flash("Members successfully officially logged into the Founding Resolution!", "success")
            return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
            
    # Fallback or "new" resolution logic
    for claim in claims:
        claim.status = "PENDING_RESOLUTION"
        
    res_count = UipResolution.query.filter_by(organization_id=org.id).count() + 1
    
    res = UipResolution(
        organization_id=org.id,
        title=f"Resolution {current_year}-{res_count} - Access Bundle",
        description="Resolution to grant active platform access to the bundled applicants.\\n",
        status="PROPOSED",
        recorded_by=current_user.id,
        result_basis={"type": "access_bundle", "interaction_ids": [c.id for c in claims]}
    )
    for claim in claims:
        role_name = claim.interaction_type.replace('_claim', '').title()
        res.description += f"\\n- {claim.creator.name}: {role_name}"
        
    db.session.add(res)"""

if old_portfolio_logic in text:
    text = text.replace(old_portfolio_logic, new_portfolio_logic)
    print("Replaced backend portfolio logic")
else:
    print("Failed to replace backend portfolio logic!")

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
