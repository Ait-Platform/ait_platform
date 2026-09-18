import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_block = """            singular_roles = ["chairman", "vice chairman", "secretary", "treasurer"]
            if position.lower() in singular_roles:
                occupied = UipCommitteeMember.query.filter(
                    UipCommitteeMember.organization_id == org.id,
                    func.lower(UipCommitteeMember.position) == position.lower(),
                    UipCommitteeMember.status == "CURRENT"
                ).first()
                if occupied:
                    flash(f"The position of {position} is currently occupied. Please click the 'Unsure / Other' tile and the Secretary will sort it out.", "warning")
                    return redirect(url_for("uip_bp.router_page", org_slug=org.slug))"""

new_block = """            singular_roles = ["chairman", "vice chairman", "secretary", "treasurer"]
            if position.lower() in singular_roles:
                occupied = UipCommitteeMember.query.filter(
                    UipCommitteeMember.organization_id == org.id,
                    func.lower(UipCommitteeMember.position) == position.lower(),
                    UipCommitteeMember.status == "CURRENT"
                ).first()
                
                # GENESIS SECRETARY LOGIC
                if position.lower() == "secretary" and not occupied:
                    new_sec = UipCommitteeMember(
                        organization_id=org.id,
                        user_id=current_user.id,
                        term_id=term.id if term else None,
                        name=current_user.name,
                        email=current_user.email,
                        level=level,
                        position=position,
                        status="CURRENT"
                    )
                    db.session.add(new_sec)
                    
                    # Ensure membership and role assignment
                    from app.models.core import CoreOrganizationMember, CoreRoleAssignment, CoreRole
                    membership = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=current_user.id).first()
                    if not membership:
                        membership = CoreOrganizationMember(organization_id=org.id, user_id=current_user.id, is_active=True)
                        db.session.add(membership)
                        db.session.flush()
                    
                    role = CoreRole.query.filter_by(organization_id=org.id, slug="committee_member").first()
                    if role:
                        if not CoreRoleAssignment.query.filter_by(member_id=membership.id, role_id=role.id).first():
                            assign = CoreRoleAssignment(member_id=membership.id, role_id=role.id)
                            db.session.add(assign)
                    
                    db.session.commit()
                    flash("Genesis Admin initialized. Welcome to your Secretary dashboard.", "success")
                    return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))

                if occupied:
                    flash(f"The position of {position} is currently occupied. Please click the 'Unsure / Other' tile and the Secretary will sort it out.", "warning")
                    return redirect(url_for("uip_bp.router_page", org_slug=org.slug))"""

text = text.replace(old_block, new_block)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated Genesis Secretary logic")
