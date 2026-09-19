with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """                    # AUTO-GENERATE THE 4 FOUNDATIONAL RESOLUTIONS
                    from app.models.uip import UipResolution
                    from sqlalchemy import text
                    
                    meeting_res = db.session.execute(text("SELECT id FROM uip_committee_meeting WHERE organization_id = :org_id AND meeting_type = 'FOUNDING' LIMIT 1"), {"org_id": org.id}).fetchone()
                    if meeting_res:
                        meeting_id_val = meeting_res[0]
                    else:
                        result = db.session.execute(text("INSERT INTO uip_committee_meeting (organization_id, title, meeting_type, scheduled_at, status) VALUES (:org_id, 'Precinct Founding Meeting', 'FOUNDING', CURRENT_TIMESTAMP, 'CONCLUDED') RETURNING id"), {"org_id": org.id})
                        meeting_id_val = result.scalar()

                    foundational_resolutions = [
                        {"title": "Founding Declaration", "desc": "Formal establishment of the Precinct and adoption of the constitution."},
                        {"title": "Access Bundle", "desc": "Batched approval of initial verified members and ratepayers."},
                        {"title": "Manager Designation", "desc": "Delegation of operational authority to precinct staff and supervisors."},
                        {"title": "Token Wallet Authorization", "desc": "Adoption of the AIT platform and authorization of token expenditure."}
                    ]
                    for res_data in foundational_resolutions:
                        new_res = UipResolution(
                            organization_id=org.id,
                            meeting_id=meeting_id_val,
                            title=res_data["title"],
                            description=res_data["desc"],
                            status="DRAFT",
                            voting_scope="EXCO",
                            recorded_by=current_user.id
                        )
                        db.session.add(new_res)"""

new_logic = """                    # AUTO-GENERATE THE 4 FOUNDATIONAL RESOLUTIONS (IDEMPOTENT)
                    from app.models.uip import UipResolution
                    from sqlalchemy import text
                    
                    # Ensure we only generate them if the resolution register is completely empty
                    if UipResolution.query.filter_by(organization_id=org.id).count() == 0:
                        meeting_res = db.session.execute(text("SELECT id FROM uip_committee_meeting WHERE organization_id = :org_id AND meeting_type = 'FOUNDING' LIMIT 1"), {"org_id": org.id}).fetchone()
                        if meeting_res:
                            meeting_id_val = meeting_res[0]
                        else:
                            result = db.session.execute(text("INSERT INTO uip_committee_meeting (organization_id, title, meeting_type, scheduled_at, status) VALUES (:org_id, 'Precinct Founding Meeting', 'FOUNDING', CURRENT_TIMESTAMP, 'CONCLUDED') RETURNING id"), {"org_id": org.id})
                            meeting_id_val = result.scalar()
    
                        foundational_resolutions = [
                            {"title": "Founding Declaration", "desc": "Formal establishment of the Precinct and adoption of the constitution."},
                            {"title": "Access Bundle", "desc": "Batched approval of initial verified members and ratepayers."},
                            {"title": "Manager Designation", "desc": "Delegation of operational authority to precinct staff and supervisors."},
                            {"title": "Token Wallet Authorization", "desc": "Adoption of the AIT platform and authorization of token expenditure."}
                        ]
                        for res_data in foundational_resolutions:
                            new_res = UipResolution(
                                organization_id=org.id,
                                meeting_id=meeting_id_val,
                                title=res_data["title"],
                                description=res_data["desc"],
                                status="DRAFT",
                                voting_scope="EXCO",
                                recorded_by=current_user.id
                            )
                            db.session.add(new_res)"""

text = text.replace(old_logic, new_logic)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added idempotency check to auto-generation")
