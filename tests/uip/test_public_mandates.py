"""Public mandate ordering uses recorded model timestamps, with no writes."""
from datetime import date, datetime
import sqlalchemy as sa
from bootstrap import db,uip

def test_public_mandates_scoped_ordering(client,data):
    meeting=uip.UipCommitteeMeeting(organization_id=data.org.id,title="Recorded meeting",scheduled_at=datetime(2026,1,1),status="CONCLUDED")
    db.session.add(meeting);db.session.flush()
    rows=[uip.UipResolution(organization_id=data.org.id,meeting_id=meeting.id,title=title,status=status,decision_date=decision,created_at=created) for title,status,decision,created in (
        ("Older creation","ADOPTED",date(2026,1,2),datetime(2026,1,1)),
        ("Newer creation","ADOPTED",date(2026,1,2),datetime(2026,1,2)),
        ("Later decision","ADOPTED",date(2026,1,3),datetime(2026,1,1)),
        ("Undated mandate","ADOPTED",None,datetime(2026,1,4)),
        ("Unadopted private","PROPOSED",date(2026,1,5),datetime(2026,1,5)))]
    db.session.add_all(rows);db.session.commit()
    statements=[];connection=db.session.connection()
    def capture(conn,cursor,statement,*args):statements.append(statement)
    sa.event.listen(connection,"before_cursor_execute",capture)
    try:
        with client.session_transaction() as session:session.clear()
        from flask import g
        g.pop("_login_user",None)
        response=client.get("/uip/manor-gardens/public-mandates")
        assert response.status_code==200
        body=response.get_data(as_text=True)
        assert body.index("Later decision") < body.index("Newer creation") < body.index("Older creation") < body.index("Undated mandate")
        assert "Unadopted private" not in body
        assert not any(s.lstrip().split()[0].upper() in {"INSERT","UPDATE","DELETE","CREATE","ALTER","DROP"} for s in statements)
    finally:sa.event.remove(connection,"before_cursor_execute",capture)
