import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Replace the resolutions injection block to include a meeting.
old_inj = """                foundational_resolutions = [
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
                db.session.commit()"""

new_inj = """                from datetime import datetime
                from app.models.uip import UipCommitteeMeeting
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

text = text.replace(old_inj, new_inj)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated resolutions injection to include meeting")
