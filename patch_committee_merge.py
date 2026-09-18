import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Fix the role_slug and position logic in committee_routes
old_role_logic = """            # Ensure they have an active organization membership
            org_mem = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=claim.creator.id).first()
            if not org_mem:
                org_mem = CoreOrganizationMember(organization_id=org.id, user_id=claim.creator.id, is_active=True)
                db.session.add(org_mem)
            else:
                org_mem.is_active = True
                
            # Grant the role
            role_slug = "committee_member" if "committee" in claim.interaction_type else "mo" if "mo" in claim.interaction_type else "ratepayer"
            role_obj = CoreRole.query.filter_by(slug=role_slug).first()
            if role_obj:
                existing_role = CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id).first()
                if not existing_role:
                    db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id))

            if 'committee' in claim.interaction_type:"""

new_role_logic = """            from app.models.uip import UipMemberProfile
            from sqlalchemy import func
            
            # 1. Ensure they have an active organization membership
            org_mem = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=claim.creator.id).first()
            
            if not org_mem:
                # 2. Check the Vault to see if there is a Ghost Member to merge!
                vault_record = UipMemberProfile.query.filter(
                    UipMemberProfile.organization_id == org.id,
                    func.lower(UipMemberProfile.email) == func.lower(claim.creator.email)
                ).first()
                
                if vault_record and vault_record.membership_id:
                    ghost_mem = CoreOrganizationMember.query.get(vault_record.membership_id)
                    if ghost_mem and ghost_mem.user_id is None:
                        # MERGE
                        ghost_mem.user_id = claim.creator.id
                        ghost_mem.is_active = True
                        org_mem = ghost_mem
                        vault_record.eligibility_status = "eligible"
                
                if not org_mem:
                    org_mem = CoreOrganizationMember(organization_id=org.id, user_id=claim.creator.id, is_active=True)
                    db.session.add(org_mem)
            else:
                org_mem.is_active = True
                
            # Grant the role
            role_slug = "committee_member" if claim.interaction_type in ["committee_claim", "secretary_claim", "chairman_claim", "treasurer_claim"] else "mo" if "mo" in claim.interaction_type else "ratepayer"
            role_obj = CoreRole.query.filter_by(slug=role_slug).first()
            if role_obj:
                existing_role = CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id).first()
                if not existing_role:
                    db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id))

            if claim.interaction_type in ["committee_claim", "secretary_claim", "chairman_claim", "treasurer_claim"]:"""
text = text.replace(old_role_logic, new_role_logic)

# Fix position
old_pos = """                mem = UipCommitteeMember(
                    organization_id=org.id,
                    user_id=claim.creator.id,
                    name=claim.creator.name,
                    email=claim.creator.email,
                    position="Unassigned",
                    status="CURRENT"
                )"""

new_pos = """                pos = "Secretary" if claim.interaction_type == "secretary_claim" else "Unassigned"
                mem = UipCommitteeMember(
                    organization_id=org.id,
                    user_id=claim.creator.id,
                    name=claim.creator.name,
                    email=claim.creator.email,
                    position=pos,
                    status="CURRENT"
                )"""
text = text.replace(old_pos, new_pos)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched committee_routes merge logic and role assignment")
