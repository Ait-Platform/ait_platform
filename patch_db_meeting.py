from app import create_app
from app.extensions import db
from app.models.uip import UipCommitteeMeeting
import datetime

app = create_app()
with app.app_context():
    org_id = 1
    meetings = UipCommitteeMeeting.query.filter_by(organization_id=org_id, title="Founding AGM").all()
    for m in meetings:
        m.status = "SCHEDULED"
        # Push it into the future so it makes sense
        m.scheduled_at = datetime.datetime(2026, 10, 1, 18, 0)
    db.session.commit()
    print("Fixed existing database meeting statuses!")
