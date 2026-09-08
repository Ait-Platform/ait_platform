from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from bootstrap import db, core, User
from app.uip.services import governance, sla
from test_work_order_concurrency import concurrent_db, parallel
from test_phase49 import eligible_member


def test_concurrent_survey_response_records_one_eligible_vote(concurrent_db):
    app, engine, ids = concurrent_db
    with app.app_context():
        issue = db.session.get(core.CoreInteraction, ids.issue)
        data = SimpleNamespace(org=db.session.get(core.CoreOrganization, ids.org),
            users={"manager": db.session.get(User, ids.manager), "resident": db.session.get(User, issue.creator_id)})
        member = eligible_member(data)
        now = datetime.now(timezone.utc)
        row = governance.survey(ids.org, ids.manager, dict(title="Concurrent", purpose="One response", relationship="owner",
            opens_at=(now - timedelta(minutes=1)).isoformat(), closes_at=(now + timedelta(hours=1)).isoformat(), identifiable="no"),
            [dict(title="Approve?", type="YES_NO")])
        survey_id, member_id, actor = row.id, member.id, issue.creator_id
        db.session.commit()
        db.session.remove()
    results = parallel(app, [lambda: governance.respond(ids.org, actor, survey_id, member_id, {"1": "Yes"}),
                             lambda: governance.respond(ids.org, actor, survey_id, member_id, {"1": "No"})])
    assert sorted(status for status, row_id in results) == [200, 409]
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT count(*) FROM uip_survey_response").scalar() == 1


def test_concurrent_sla_configuration_retains_one_active_policy(concurrent_db):
    app, engine, ids = concurrent_db
    results = parallel(app, [lambda: sla.configure(ids.org, ids.manager, "SECURITY", "NORMAL", "dispatch", 60, 10),
                             lambda: sla.configure(ids.org, ids.manager, "SECURITY", "NORMAL", "dispatch", 90, 10)])
    assert [status for status, row_id in results] == [200, 200]
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT count(*) FROM uip_sla_policy WHERE is_active").scalar() == 1
        assert connection.exec_driver_sql("SELECT count(*) FROM uip_sla_policy").scalar() == 2
