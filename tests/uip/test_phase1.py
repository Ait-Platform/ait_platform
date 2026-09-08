"""Sixteen persisted Phase 1 security and behaviour regression checks."""
from bootstrap import db, core, uip
BASE = "/uip/manor-gardens"
ISSUE = BASE + "/interaction/MG-TEST"
PAYLOAD = dict(title="Safety", description="Test issue", category="SECURITY", channel="Telephone", priority="NORMAL")


def test_01_manager_dashboard(client):
    response = client.get(BASE + "/dashboard")
    assert response.status_code == 200
    assert b"Test issue" in response.data
    assert b"Attention Required" in response.data
    assert b"unavailable" not in response.data.lower()


def test_02_intake_and_settings_templates(client):
    assert client.get(BASE + "/interaction/new").status_code == 200
    assert client.get(BASE + "/settings").status_code == 200


def test_03_detail_uses_user_name(client):
    response = client.get(ISSUE)
    assert response.status_code == 200
    assert b"Resident" in response.data


def test_04_csrf_missing_denied(client):
    assert client.post(BASE + "/interaction/new", data=PAYLOAD).status_code == 400


def test_05_csrf_valid_intake(client):
    assert client.safe_post(BASE + "/interaction/new", PAYLOAD).status_code == 302
    assert core.CoreInteraction.query.filter_by(title="Safety").one().status == "NEW"


def test_06_unknown_org_no_provision(client):
    count = core.CoreOrganization.query.count()
    assert client.get("/uip/missing/dashboard").status_code == 404
    assert core.CoreOrganization.query.count() == count


def test_07_outsider_denied(client):
    client.login("outsider")
    assert client.get(BASE + "/dashboard").status_code == 403


def test_08_inactive_membership_denied(client, data):
    core.CoreOrganizationMember.query.filter_by(user_id=data.users["manager"].id).one().is_active = False
    db.session.commit()
    assert client.get(BASE + "/dashboard").status_code == 403


def test_09_cross_org_issue_hidden(client):
    assert client.get(BASE + "/interaction/OTHER-TEST").status_code == 404


def test_10_resident_only_own_issue(client, data):
    client.login("resident")
    assert client.get(ISSUE).status_code == 200
    data.issue.creator_id = data.users["manager"].id; db.session.commit()
    assert client.get(ISSUE).status_code == 403


def test_11_provider_task_never_grants_issue_access(client, data):
    client.login("provider")
    assert client.get(ISSUE).status_code == 403
    db.session.add(core.CoreTask(interaction_id=data.issue.id, assignee_id=data.users["provider"].id, title="Assigned")); db.session.commit()
    assert client.get(ISSUE).status_code == 403


def test_12_cross_org_assignee_rejected(client, data):
    assert client.safe_post(ISSUE + "/task", dict(title="Task", assignee_id=data.outsider.id)).status_code == 400
    assert core.CoreTask.query.count() == 0


def test_13_cross_org_provider_rejected(client, data):
    provider = uip.UipProvider(organization_id=data.other.id, name="Other provider", is_active=True)
    db.session.add(provider); db.session.commit()
    assert client.safe_post(ISSUE + "/provider", dict(provider_id=provider.id)).status_code == 400
    assert uip.UipWorkOrder.query.count() == 0


def test_14_maintenance_inaccessible(client):
    for path in ("seed", "setup", "fix_roles", "maintenance", "demo", "patch_db"):
        assert client.get(BASE + "/" + path).status_code == 404


def test_15_ai_unavailable_no_wallet_write(client):
    assert client.safe_post(ISSUE + "/summarize").status_code == 302
    assert core.CoreOrganizationWallet.query.count() == 0
    assert core.CoreAiRequest.query.count() == 0


def test_16_existing_task_and_resolution(client, data):
    assert client.safe_post(ISSUE + "/task", dict(title="Task", assignee_id=data.users["receptionist"].id)).status_code == 302
    task = core.CoreTask.query.one()
    assert client.safe_post(BASE + "/task/" + str(task.id) + "/complete").status_code == 302
    assert task.status == "completed"
    assert client.safe_post(ISSUE + "/resolve").status_code == 302
    assert data.issue.status == "RESOLVED"
