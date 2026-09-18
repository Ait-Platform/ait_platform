import re
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Fix role assignment
old_role = """role_slug = "committee_member" if "committee" in claim.interaction_type else "mo" if "mo" in claim.interaction_type else "ratepayer\""""
new_role = """role_slug = "committee_member" if claim.interaction_type in ["committee_claim", "secretary_claim", "chairman_claim", "treasurer_claim"] else "mo" if "mo" in claim.interaction_type else "ratepayer\""""
text = text.replace(old_role, new_role)

# Fix position assignment
old_pos = """                    mem = UipCommitteeMember(
                        organization_id=org.id,
                        user_id=claim.creator.id,
                        name=claim.creator.name,
                        email=claim.creator.email,
                        position="Unassigned",
                        status="CURRENT"
                    )"""

new_pos = """                    pos = "Secretary" if claim.interaction_type == "secretary_claim" else "Unassigned"
                    mem = UipCommitteeMember(
                        organization_id=org.id,
                        user_id=claim.creator.id,
                        name=claim.creator.name,
                        email=claim.creator.email,
                        position=pos,
                        status="CURRENT"
                    )"""
text = text.replace(old_pos, new_pos)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched role and position logic")
