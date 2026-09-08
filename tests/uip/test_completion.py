"""Visible UIP journeys using real routes, CSRF, services and disposable schemas."""
import csv
import io
import re
import html
from datetime import datetime, timedelta, timezone

import pytest
from bootstrap import db, core, uip
from test_register import MEMBER, PROPERTY
from phase3_helpers import key

BASE = "/uip/manor-gardens"


def links(response):
    return [html.unescape(s) for s in re.findall(r'href="([^"]+)"', response.get_data(as_text=True))]


def visit_link(client, response, suffix):
    target = next((s for s in links(response) if suffix in s), None)
    assert target, (suffix, links(response))
    result = client.get(target)
    assert result.status_code == 200, (target, result.status_code)
    return result


@pytest.mark.parametrize("role", ["manager", "receptionist", "committee_member", "owner", "resident", "provider"])
def test_visible_navigation_matches_permissions(client, role):
    client.login(role)
    response = client.get(BASE + "/dashboard", follow_redirects=True)
    assert response.status_code == 200
    nav = re.search(r'<nav aria-label="UIP Command Centre".*?</nav>', response.get_data(as_text=True), re.S)
    assert nav
    for target in re.findall(r'href="([^"]+)"', nav.group()):
        result = client.get(html.unescape(target), follow_redirects=True)
        assert result.status_code == 200, (role, target, result.status_code)


def test_intake_registration_roundtrip_and_empty_states(client, data):
    response = client.get(BASE + "/interaction/new")
    assert b"No registered ratepayers yet" in response.data
    assert b"No registered properties yet" in response.data
    assert b'name="resident_email"' not in response.data
    member_url = next(p for p in links(response) if "/members/new?" in p)
    saved = client.safe_post(member_url, MEMBER)
    assert saved.status_code == 302 and "/interaction/new?member_id=" in saved.location
    response = client.get(saved.location)
    member = uip.UipMemberProfile.query.one()
    assert f'value="{member.id}" selected'.encode() in response.data
    property_url = next(p for p in links(response) if "/properties/new?" in p)
    saved = client.safe_post(property_url, PROPERTY)
    assert saved.status_code == 302 and "member_id=" in saved.location and "property_id=" in saved.location
    assert client.get(saved.location).status_code == 200
    assert member.membership.user_id is None


def csv_file(kind, rows):
    from app.uip.completion_routes import CSV_COLUMNS
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=CSV_COLUMNS[kind])
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode()


def csv_post(client, kind, content, **values):
    return client.safe_post(BASE + "/register/import", dict(kind=kind,
        file=(io.BytesIO(content), "register.csv"), **values))


def preview_token(response):
    match = re.search(rb'name="preview_token" value="([^"]+)"', response.data)
    assert match, response.get_data(as_text=True)
    return match[1].decode()


def test_csv_preview_commit_duplicate_and_atomicity(client, data):
    content = csv_file("members", [MEMBER])
    before = uip.UipAuditEvent.query.count()
    preview = csv_post(client, "members", content, operation="preview")
    assert preview.status_code == 200
    assert uip.UipMemberProfile.query.count() == 0 and uip.UipAuditEvent.query.count() == before
    token = preview_token(preview)
    assert csv_post(client, "members", content + b"\n", operation="commit", preview_token=token).status_code == 400
    assert csv_post(client, "members", content, operation="commit", preview_token=token).status_code == 302
    assert uip.UipMemberProfile.query.count() == 1
    assert csv_post(client, "members", content, operation="commit", preview_token=token).status_code == 400
    duplicate = csv_file("members", [{**MEMBER, "reference": "M2", "email": "two@example.invalid"}, MEMBER])
    assert csv_post(client, "members", duplicate, operation="preview").status_code == 400
    assert uip.UipMemberProfile.query.count() == 1
    for kind, rows in (("properties", [PROPERTY]), ("relationships", [dict(member_reference="M1", property_reference="P1",
            relationship="owner", valid_from="2026-01-01", valid_to="", is_verified="false")])):
        content = csv_file(kind, rows)
        token = preview_token(csv_post(client, kind, content, operation="preview"))
        assert csv_post(client, kind, content, operation="commit", preview_token=token).status_code == 302
    assert uip.UipPropertyMember.query.count() == 1


def test_import_scope_and_permissions(client, data):
    from app.uip.services import register
    register.save_member(data.other.id, data.outsider.id, MEMBER)
    register.save_property(data.other.id, data.outsider.id, PROPERTY)
    db.session.commit()
    content = csv_file("relationships", [dict(member_reference="M1", property_reference="P1",
        relationship="owner", valid_from="2026-01-01", valid_to="", is_verified="true")])
    result = csv_post(client, "relationships", content, operation="preview")
    assert result.status_code == 400 and b"this organisation" in result.data
    for role in ("provider", "resident", "receptionist"):
        client.login(role)
        assert client.get(BASE + "/register/import").status_code == 403
    client.login("manager")
    assert client.post(BASE + "/register/import", data={}).status_code == 400


def test_manager_full_visible_operational_journey(client, data, app, tmp_path, monkeypatch):
    """The 13 requested steps; only existing authorised accounts are fixture setup."""
    app.instance_path = str(tmp_path)
    dashboard = client.get(BASE + "/dashboard")
    settings = visit_link(client, dashboard, "/settings")
    setup = visit_link(client, settings, "/getting-started")
    visit_link(client, setup, "/members/new")
    membership = core.CoreOrganizationMember.query.filter_by(organization_id=data.org.id,
        user_id=data.users["resident"].id).one()
    # The account is optional; the separate roundtrip test proves non-login capture.
    assert client.safe_post(BASE + "/members/new", dict(MEMBER, membership_id=membership.id,
        eligibility_status="eligible")).status_code == 302
    member = uip.UipMemberProfile.query.one()
    member_page = client.get(BASE + f"/members/{member.id}")
    visit_link(client, member_page, "/properties/new")
    saved = client.safe_post(BASE + f"/properties/new?member_id={member.id}", PROPERTY)
    prop_page = client.get(saved.location)
    prop = uip.UipProperty.query.one()
    assert b"Add relationship" in prop_page.data
    assert client.safe_post(BASE + f"/properties/{prop.id}/members", dict(member_id=member.id,
        relationship="owner", valid_from="2026-01-01", valid_to="", is_verified="true")).status_code == 302
    visit_link(client, member_page, "/interaction/new")
    assert client.safe_post(BASE + "/operations/sla", dict(category="SECURITY", priority="NORMAL",
        stage="acknowledgement", target_minutes="60", warning_minutes="15")).status_code == 302
    saved = client.safe_post(BASE + "/interaction/new", dict(member_id=member.id, property_id=prop.id,
        title="Gate repair", description="Repair the entrance gate", category="SECURITY", priority="NORMAL", channel="Reception"))
    issue_page = client.get(saved.location)
    issue = core.CoreInteraction.query.filter_by(title="Gate repair").one()
    assert b"Create provider work order" in issue_page.data
    activity = visit_link(client, issue_page, f"/operations/reception/{issue.id}")
    activity_url = BASE + f"/operations/reception/{issue.id}"
    assert client.safe_post(activity_url, dict(operation="task", title="Inspect gate")).status_code == 302
    task = core.CoreTask.query.filter_by(interaction_id=issue.id).one()
    assert client.safe_post(BASE + f"/task/{task.id}/complete", {}).status_code == 302
    visit_link(client, dashboard, "/providers")
    assert client.safe_post(BASE + "/providers/new", dict(name="Garden repairs", availability="AVAILABLE",
        capabilities="SECURITY", contact_email="provider@example.invalid", contact_phone="")).status_code == 302
    provider = uip.UipProvider.query.one()
    provider_member = core.CoreOrganizationMember.query.filter_by(organization_id=data.org.id,
        user_id=data.users["provider"].id).one()
    assert client.safe_post(BASE + f"/providers/{provider.id}/users", dict(membership_id=provider_member.id,
        expected_version=provider.version)).status_code == 302
    assert client.safe_post(BASE + f"/interaction/{issue.reference}/provider", dict(provider_id=provider.id,
        description="Repair gate", service_location="Entrance", request_key=key())).status_code == 302
    order = uip.UipWorkOrder.query.one()
    for action, role, extras in (("dispatched", "manager", {"dispatch_method": "TELEPHONE"}),
        ("accepted", "provider", {}), ("started", "provider", {}),
        ("completed", "provider", {"note": "Gate repaired"}), ("verified", "manager", {}), ("closed", "manager", {})):
        client.login(role)
        order_page = client.get(BASE + f"/work-orders/{order.id}")
        assert f'value="{action}"'.encode() in order_page.data
        result = client.safe_post(BASE + f"/work-orders/{order.id}/actions", dict(action=action,
            expected_version=order.version, request_key=key(), **extras))
        assert result.status_code == 302, result.get_data(as_text=True)
    assert issue.status == "RESOLVED"
    assert client.get(BASE + "/operations/sla").status_code == 200
    # Municipal and contact work is attached to the still-open fixture issue.
    activity_url = BASE + f"/operations/reception/{data.issue.id}"
    for values in (dict(operation="acknowledge"), dict(operation="follow_up", method="INTERNAL",
        outcome="INFORMATION_RECEIVED", next_action="NONE", note="Received update"),
        dict(operation="referral", department="Roads"), dict(operation="communication", channel="EMAIL",
        direction="OUTBOUND", party_classification="MUNICIPALITY", purpose="FOLLOW_UP", status="RECORDED", summary="Call recorded")):
        assert client.safe_post(activity_url, values).status_code == 302
    assert client.get(BASE + "/operations/municipal").status_code == 200
    assert client.get(BASE + "/operations/communications").status_code == 200
    doc_page = visit_link(client, dashboard, "/operations/documents")
    assert b"Upload controlled document" in doc_page.data
    assert client.safe_post(BASE + "/operations/documents", dict(operation="upload", title="Policy", category="Governance",
        access_classification="MEMBERS", effective_date="2026-01-01", file=(io.BytesIO(b"Policy text"), "policy.txt"))).status_code == 302
    visit_link(client, dashboard, "/operations/meetings")
    assert client.safe_post(BASE + "/operations/meetings", dict(operation="rule", percentage="50", minimum="1", relationship="owner")).status_code == 302
    now = datetime.now(timezone.utc)
    assert client.safe_post(BASE + "/operations/meetings", dict(operation="meeting", title="AGM", meeting_type="AGM",
        scheduled_at=(now - timedelta(minutes=5)).isoformat(), location="Hall", agenda="Gate policy")).status_code == 302
    meeting = uip.UipCommitteeMeeting.query.one()
    url = BASE + f"/operations/meetings/{meeting.id}"
    for values in (dict(operation="attendance", member_id=member.id, status="INVITED"), dict(operation="start"),
        dict(operation="attendance", member_id=member.id, status="PRESENT"), dict(operation="conclude", minutes="Policy adopted")):
        assert client.safe_post(url, values).status_code == 302
    concluded = client.get(url)
    visit_link(client, concluded, "/operations/decisions?")
    assert meeting.quorum_achieved
    assert client.safe_post(BASE + "/operations/decisions", dict(operation="decision", meeting_id=meeting.id,
        title="Gate policy", description="Adopt policy", votes_for="1", votes_against="0", abstentions="0", linked_task_id=task.id)).status_code == 302
    assert client.safe_post(BASE + "/operations/surveys", dict(title="Poll", purpose="Review policy", relationship="owner",
        identifiable="no", opens_at=(now - timedelta(minutes=1)).isoformat(), closes_at=(now + timedelta(minutes=10)).isoformat(),
        question_1="Approve?", type_1="YES_NO")).status_code == 302
    survey = uip.UipSurvey.query.one()
    client.login("resident")
    assert b"Submit one response" in client.get(BASE + f"/operations/surveys/{survey.id}").data
    assert client.safe_post(BASE + f"/operations/surveys/{survey.id}", dict(operation="respond", member_id=member.id, answer_1="Yes")).status_code == 302
    class Later(datetime):
        @classmethod
        def now(cls, tz=None):
            return now + timedelta(minutes=11)
    from app.uip.services import governance
    from app.uip import operational_routes
    monkeypatch.setattr(governance, "datetime", Later)
    monkeypatch.setattr(operational_routes, "datetime", Later)
    client.login("manager")
    survey_page = client.get(BASE + f"/operations/surveys/{survey.id}")
    assert b"Finalize results" in survey_page.data
    assert client.safe_post(BASE + f"/operations/surveys/{survey.id}", dict(operation="finalize")).status_code == 302
    for path in ("/operations/decisions", "/reports", "/operations/report.csv", "/audit"):
        assert client.get(BASE + path).status_code == 200
    assert survey.results["response_count"] == 1


def test_document_metadata_preserves_versions(client, data, app, tmp_path):
    app.instance_path = str(tmp_path)
    assert client.safe_post(BASE + "/operations/documents", dict(title="Policy", category="Policies",
        access_classification="MEMBERS", effective_date="2026-01-01", file=(io.BytesIO(b"original"), "policy.txt"))).status_code == 302
    doc = uip.UipDocument.query.one()
    version = uip.UipDocumentVersion.query.one()
    original = version.sha256
    assert client.safe_post(BASE + f"/operations/documents/{doc.id}", dict(operation="metadata",
        title="Renamed", category="Committee", access_classification="PRIVATE")).status_code == 302
    assert doc.title == "Renamed" and doc.current_version == 1 and version.sha256 == original
    client.login("resident")
    assert client.get(BASE + f"/operations/documents/{doc.id}").status_code == 404


def test_meeting_edit_cancel_and_sla_disable(client, data):
    now = datetime.now(timezone.utc)
    values = dict(operation="meeting", title="Planning", meeting_type="Committee", location="Hall",
                  agenda="Plan", scheduled_at=(now + timedelta(days=1)).isoformat())
    assert client.safe_post(BASE + "/operations/meetings", values).status_code == 302
    row = uip.UipCommitteeMeeting.query.one()
    url = BASE + f"/operations/meetings/{row.id}"
    assert client.safe_post(url, dict(values, operation="edit", title="Revised plan")).status_code == 302
    assert row.title == "Revised plan"
    assert client.safe_post(url, dict(operation="cancel", reason="Rescheduling")).status_code == 302
    assert row.status == "CANCELLED"
    assert client.safe_post(url, dict(values, operation="edit")).status_code == 409
    assert client.safe_post(BASE + "/operations/sla", dict(category="SECURITY", priority="NORMAL",
        stage="dispatch", target_minutes="30", warning_minutes="5")).status_code == 302
    policy = uip.UipSlaPolicy.query.one()
    assert client.safe_post(BASE + "/operations/sla", dict(operation="deactivate", policy_id=policy.id)).status_code == 302
    assert not policy.is_active


def test_relationship_history_and_overlap(client, data):
    assert client.safe_post(BASE + "/members/new", MEMBER).status_code == 302
    assert client.safe_post(BASE + "/properties/new", PROPERTY).status_code == 302
    member, prop = uip.UipMemberProfile.query.one(), uip.UipProperty.query.one()
    values = dict(member_id=member.id, relationship="owner", valid_from="2026-01-01", valid_to="", is_verified="true")
    url = BASE + f"/properties/{prop.id}/members"
    assert client.safe_post(url, values).status_code == 302
    row = uip.UipPropertyMember.query.one()
    assert client.safe_post(url, dict(values, valid_from="2026-02-01")).status_code == 409
    assert client.safe_post(url + f"/{row.id}", dict(values, valid_from="2025-01-01")).status_code == 400
    assert client.safe_post(url + f"/{row.id}", dict(values, valid_to="2026-02-01")).status_code == 302
    assert client.safe_post(url + f"/{row.id}", values).status_code == 400
    assert client.safe_post(url, dict(values, valid_from="2026-02-02")).status_code == 302
    assert uip.UipPropertyMember.query.count() == 2
