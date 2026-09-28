import re

filepath = 'app/program_uip/committee_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

bad_init = '''            # Create new Foundational Mandate
            res = UipResolution(
                organization_id=org.id,
                creator_id=current_user.id,
                title=title_selection,
                description="Please refer to the official attached mandate document for full details.",
                status="ADOPTED",
                voting_scope="PUBLIC",
                decision_date=datetime.utcnow().date(),
                reference="FOUNDATIONAL",
                yea_tally=0,
                nay_tally=0
            )'''

good_init = '''            # Create new Foundational Mandate
            from app.models.uip import UipCommitteeMeeting
            meeting = UipCommitteeMeeting.query.filter_by(organization_id=org.id).first()
            if not meeting:
                meeting = UipCommitteeMeeting(
                    organization_id=org.id, 
                    title="Mandate Recording Meeting", 
                    scheduled_date=datetime.utcnow(),
                    status="COMPLETED"
                )
                db.session.add(meeting)
                db.session.flush()
                
            res = UipResolution(
                organization_id=org.id,
                recorded_by=current_user.id,
                meeting_id=meeting.id,
                title=title_selection,
                description="Please refer to the official attached mandate document for full details.",
                status="ADOPTED",
                voting_scope="PUBLIC",
                decision_date=datetime.utcnow().date()
            )'''

content = content.replace(bad_init, good_init)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Route fixed")
