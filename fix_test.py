import sys
import re
with open("tests/uip/test_subcomm_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

c = re.sub(
    r"def make_resolution\(org_id, user_id, desc, status\):.*?return res",
    """def make_resolution(org_id, recorded_by, title, status="ADOPTED"):
    from app.models.uip import UipCommitteeMeeting
    from datetime import datetime
    meeting = UipCommitteeMeeting.query.filter_by(organization_id=org_id).first()
    if not meeting:
        meeting = UipCommitteeMeeting(organization_id=org_id, title="Test", meeting_type="FOUNDING", scheduled_at=datetime(2026,1,1), status="CONCLUDED")
        db.session.add(meeting)
        db.session.flush()
    res = UipResolution(organization_id=org_id, meeting_id=meeting.id, description=title, status=status, recorded_by=recorded_by)
    db.session.add(res)
    db.session.flush()
    return res""", c, flags=re.DOTALL
)

with open("tests/uip/test_subcomm_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
