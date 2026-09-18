import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# I will replace the dev_upgrade_db logic with one that uses raw SQL to guarantee no SQLAlchemy weirdness.
old_logic = """                  from app.models.uip import UipCommitteeMeeting
                  meeting = UipCommitteeMeeting.query.filter_by(organization_id=org.id, meeting_type="FOUNDING").first()
                  if not meeting:
                      meeting = UipCommitteeMeeting(
                          organization_id=org.id,
                          title="Precinct Founding Meeting",
                          meeting_type="FOUNDING",
                          scheduled_at=datetime.utcnow(),
                          status="CONCLUDED"
                      )
                      db.session.add(meeting)
                      db.session.flush()
  
                  foundational_resolutions = [
                      {"title": "Founding Declaration", "desc": "Formal establishment of the Precinct and adoption of the constitution."},
                      {"title": "Access Bundle", "desc": "Batched approval of initial verified members and ratepayers."},
                      {"title": "Manager Designation", "desc": "Delegation of operational authority to precinct staff and supervisors."},
                      {"title": "Token Wallet Authorization", "desc": "Adoption of the AIT platform and authorization of token expenditure."}
                  ]
                  for res_data in foundational_resolutions:
                      new_res = UipResolution(
                          organization_id=org.id,
                          meeting_id=meeting.id,
                          title=res_data["title"],
                          description=res_data["desc"],
                          status="PROPOSED",
                          voting_scope="EXCO",
                          quorum_target=50
                      )
                      db.session.add(new_res)
                  db.session.commit()"""

new_logic = """                  from app.models.uip import UipCommitteeMeeting
                  from sqlalchemy import text
                  
                  # Create meeting if missing using raw SQL to guarantee ID retrieval
                  meeting_res = db.session.execute(text("SELECT id FROM uip_committee_meeting WHERE organization_id = :org_id AND meeting_type = 'FOUNDING' LIMIT 1"), {"org_id": org.id}).fetchone()
                  if meeting_res:
                      meeting_id_val = meeting_res[0]
                  else:
                      result = db.session.execute(text("INSERT INTO uip_committee_meeting (organization_id, title, meeting_type, scheduled_at, status) VALUES (:org_id, 'Precinct Founding Meeting', 'FOUNDING', CURRENT_TIMESTAMP, 'CONCLUDED') RETURNING id"), {"org_id": org.id})
                      meeting_id_val = result.scalar()
                      db.session.commit()
                  
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
                          status="PROPOSED",
                          voting_scope="EXCO",
                          quorum_target=50
                      )
                      db.session.add(new_res)
                  db.session.commit()"""

text = text.replace(old_logic, new_logic)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated dev_upgrade_db with raw SQL for meeting_id")
