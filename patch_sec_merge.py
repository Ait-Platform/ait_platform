import re
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_block = """                from app.models.core import CoreOrganizationMember, CoreRoleAssignment, CoreRole
                
                # Ensure they have an active organization membership
                org_mem = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=claim.creator.id).first()
                if not org_mem:
                    org_mem = CoreOrganizationMember(organization_id=org.id, user_id=claim.creator.id, is_active=True)
                    db.session.add(org_mem)
                else:
                    org_mem.is_active = True"""

new_block = """                from app.models.core import CoreOrganizationMember, CoreRoleAssignment, CoreRole
                from app.models.uip import UipMemberProfile
                from sqlalchemy import func
                
                # 1. Ensure they have an active organization membership
                org_mem = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=claim.creator.id).first()
                
                if not org_mem:
                    # 2. Check the Vault (UipMemberProfile) to see if there is a Ghost Member to merge!
                    vault_record = UipMemberProfile.query.filter(
                        UipMemberProfile.organization_id == org.id,
                        func.lower(UipMemberProfile.email) == func.lower(claim.creator.email)
                    ).first()
                    
                    if vault_record and vault_record.membership_id:
                        ghost_mem = CoreOrganizationMember.query.get(vault_record.membership_id)
                        if ghost_mem and ghost_mem.user_id is None:
                            # MERGE: Attach the real user_id to the ghost row!
                            ghost_mem.user_id = claim.creator.id
                            ghost_mem.is_active = True
                            org_mem = ghost_mem
                            # Update Vault status
                            vault_record.eligibility_status = "eligible"
                    
                    # 3. If STILL no org_mem (not in vault), create a brand new one
                    if not org_mem:
                        org_mem = CoreOrganizationMember(organization_id=org.id, user_id=claim.creator.id, is_active=True)
                        db.session.add(org_mem)
                else:
                    org_mem.is_active = True"""

text = text.replace(old_block, new_block)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched merge logic")
