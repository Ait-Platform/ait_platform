import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_sec_logic = """                    new_sec = UipCommitteeMember(
                        organization_id=org.id,
                        term_id=term.id,
                        name=current_user.name,
                        email=current_user.email,
                        position="Secretary",  # Hardcode to match DB constraint exactly
                        status="CURRENT",
                        created_by=current_user.id
                    )
                    db.session.add(new_sec)"""

new_sec_logic = """                    new_sec = UipCommitteeMember(
                        organization_id=org.id,
                        term_id=term.id,
                        name=current_user.name,
                        email=current_user.email,
                        position="Secretary",
                        status="CURRENT",
                        created_by=current_user.id
                    )
                    db.session.add(new_sec)
                    
                    # AUTO-GENERATE THE 4 FOUNDATIONAL RESOLUTIONS
                    from app.models.uip import UipResolution
                    foundational_resolutions = [
                        {"title": "Founding Declaration", "desc": "Formal establishment of the Precinct and adoption of the constitution."},
                        {"title": "Access Bundle", "desc": "Batched approval of initial verified members and ratepayers."},
                        {"title": "Manager Designation", "desc": "Delegation of operational authority to precinct staff and supervisors."},
                        {"title": "Token Wallet Authorization", "desc": "Adoption of the AIT platform and authorization of token expenditure."}
                    ]
                    for res_data in foundational_resolutions:
                        new_res = UipResolution(
                            organization_id=org.id,
                            title=res_data["title"],
                            description=res_data["desc"],
                            status="PROPOSED",
                            voting_scope="EXCO",
                            quorum_target=50
                        )
                        db.session.add(new_res)
"""

text = text.replace(old_sec_logic, new_sec_logic)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added foundational resolutions injection")
