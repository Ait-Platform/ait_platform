"""Presentation contracts and real rendered pages; synthetic local fixtures only."""
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re

from bootstrap import db, core, uip, ROOT
from app.uip.services import register, governance, sla, reception, operations
from app.uip.presentation import executive
from test_register import make_member, make_property
from phase3_helpers import provider, order, act

BASE = "/uip/manor-gardens"


def test_executive_scoped_real_data_and_no_read_mutation(client, data):
    actor = data.users["manager"].id
    before = uip.UipAuditEvent.query.count()
    result = client.get(BASE + "/dashboard")
    assert result.status_code == 200
    assert len(re.findall(rb'class="ui-stat(?: |")', result.data)) == 6
    assert b"AI Auto-Triage" not in result.data and b"No recorded sample" not in result.data
    assert b"Other issue" not in result.data and b"OTHER-TEST" not in result.data
    assert b"Set up Manor Gardens" not in result.data  # Existing operational issue.
    overview = executive(data.org.id, actor)
    assert overview["cards"][0][1] == 1
    assert len(overview["attention"]) == 1 and overview["attention"][0]["detail"] == data.issue.title
    assert not overview["performance"]
    assert uip.UipAuditEvent.query.count() == before


def test_empty_dashboard_compact_welcome(client, data):
    db.session.delete(data.issue)
    db.session.flush()
    result = client.get(BASE + "/dashboard")
    assert b"Continue setup" in result.data
    assert b"Nothing currently requires urgent attention." in result.data
    assert len(re.findall(rb'class="ui-stat(?: |")', result.data)) == 6


def test_register_search_filter_and_links(client, data):
    member = make_member(data, name="Sample Ratepayer")
    prop = make_property(data, address="22 Sample Lane")
    register.save_relationship(data.org.id, data.users["manager"].id,
        dict(member_id=member.id, relationship="owner", valid_from="2026-01-01", is_verified="true"), property_id=prop.id)
    db.session.commit()
    assert b"22 Sample Lane" in client.get(BASE + "/members?q=Sample").data
    assert b"Sample Ratepayer" not in client.get(BASE + "/members?q=unknown").data
    assert b"Sample Ratepayer" not in client.get(BASE + "/members?status=inactive").data
    assert b"Sample Ratepayer" in client.get(BASE + "/properties?q=Sample").data
    assert b"22 Sample Lane" not in client.get(BASE + "/properties?q=unknown").data


def test_issue_filters_do_not_change_lifecycle(client, data):
    assert b"Test issue" in client.get(BASE + "/operations/reception?status=unassigned").data
    assert b"Test issue" not in client.get(BASE + "/operations/reception?status=resolved").data
    row = order(data)
    act(data, row, "dispatched", dispatch_method="TELEPHONE")
    db.session.commit()
    assert b"Test issue" in client.get(BASE + "/operations/reception?status=waiting").data
    assert b"Test issue" not in client.get(BASE + "/operations/reception?status=unassigned").data
    assert data.issue.status != "RESOLVED"


def test_attention_uses_actual_clocks_and_work_actions(client, data):
    row = order(data)
    actor = data.users["manager"].id
    sla.configure(data.org.id, actor, "SECURITY", "NORMAL", "acceptance", 60, 15)
    data.issue.priority = "NORMAL"
    act(data, row, "dispatched", dispatch_method="TELEPHONE")
    task = operations.add_task(data.org.id, actor, data.issue.id, "Overdue inspection")
    task.due_date = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=1)
    overview = executive(data.org.id, actor)
    assert any(a["label"] == "Overdue task" for a in overview["attention"])
    assert not any(a["label"] == "SLA breached" for a in overview["attention"])
    assert not overview["performance"]


def test_key_pages_render_with_sample_data(client, data, tmp_path):
    """Export real rendered HTML for separate Chromium visual/viewport checks."""
    data.org.name = "Manor Gardens UIP"
    actor = data.users["manager"].id
    member = make_member(data, name="Asha Naidoo (sample)", phone="031 000 0000", eligibility_status="eligible")
    prop = make_property(data, address="22 Sample Lane", rates_reference="MG-SAMPLE-22")
    register.save_relationship(data.org.id, actor, dict(member_id=member.id, relationship="owner", valid_from="2026-01-01", is_verified="true"), property_id=prop.id)
    register.set_preference(data.org.id, actor, member.id, dict(channel="Email", preference="allowed"))
    make_member(data, reference="M2", name="Garden Court Body Corporate (sample)", member_type="business", email="court@example.invalid")
    data.issue.title = "Streetlight not working"
    data.issue.member_id, data.issue.property_id, data.issue.priority = member.id, prop.id, "HIGH"
    partner = provider(data)
    partner.name = "Precinct Services (sample)"
    work = order(data, partner)
    act(data, work, "dispatched", dispatch_method="TELEPHONE")
    act(data, work, "accepted")
    operations.add_task(data.org.id, actor, data.issue.id, "Inspect the lighting cabinet")
    reception.referral(data.org.id, actor, data.issue.id, "Electricity department")
    now = datetime.now(timezone.utc)
    governance.quorum_rule(data.org.id, actor, 50, 1, "owner")
    meeting = governance.meeting(data.org.id, actor, dict(title="Monthly precinct meeting", meeting_type="Committee",
        scheduled_at=(now + timedelta(days=3)).isoformat(), location="Community hall", agenda="Service updates and street lighting"))
    survey = governance.survey(data.org.id, actor, dict(title="Neighbourhood priorities", purpose="Choose the next precinct improvement",
        relationship="owner", opens_at=(now - timedelta(hours=1)).isoformat(), closes_at=(now + timedelta(days=7)).isoformat(), identifiable="no"),
        [dict(title="Should lighting be prioritised?", type="YES_NO")])
    db.session.commit()
    paths = {"command-centre": "/dashboard", "ratepayers": "/members", "ratepayer-detail": f"/members/{member.id}",
        "properties": "/properties", "property-detail": f"/properties/{prop.id}", "log-interaction": f"/interaction/new?member_id={member.id}",
        "issues": "/operations/reception", "providers": "/providers", "work-orders": "/work-orders", "meetings": "/operations/meetings", "surveys": "/operations/surveys"}
    destination = Path(os.environ.get("UIP_UI_RENDER_DIR", str(tmp_path)))
    if os.environ.get("UIP_UI_RENDER_DIR"):
        assert destination.resolve().is_relative_to((ROOT / "scratch").resolve())
    destination.mkdir(parents=True, exist_ok=True)
    for name, path in paths.items():
        response = client.get(BASE + path)
        assert response.status_code == 200, (path, response.status_code)
        html = response.get_data(as_text=True)
        assert html.count('aria-label="UIP Command Centre"') == 1, name
        assert len(re.findall(r"<h1(?:\s|>)", html)) == 1, name
        (destination / (name + ".html")).write_text(html, encoding="utf-8")
    assert b"AI Assist" not in client.get(BASE + "/interaction/new").data
