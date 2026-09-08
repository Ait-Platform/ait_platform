import pytest
from bootstrap import db, core, uip
from app.uip.services import work_orders, operations, providers
from phase3_helpers import provider, order, act, key
BASE="/uip/manor-gardens"


def test_provider_projection_hides_internal_graph_and_drafts(client,data):
    row=order(data)
    data.issue.description="PRIVATE ISSUE BODY"
    operations.add_task(data.org.id,data.users["manager"].id,data.issue.id,"PRIVATE INTERNAL TASK")
    db.session.commit();client.login("provider")
    assert client.get(f"{BASE}/work-orders/{row.id}").status_code==404
    assert row.reference.encode() not in client.get(BASE+"/work-orders").data
    act(data,row,"dispatched",dispatch_method="EMAIL");db.session.commit()
    response=client.get(f"{BASE}/work-orders/{row.id}")
    assert response.status_code==200
    assert b"Approved scope" in response.data and b"Public entrance" in response.data
    for private in (b"PRIVATE ISSUE BODY",b"PRIVATE INTERNAL TASK",b"Resident",b"Internal issue"):
        assert private not in response.data
    for path in ("/interaction/MG-TEST","/members","/properties","/audit","/providers"):
        assert client.get(BASE+path).status_code==403
    assert row.reference.encode() in client.get(BASE+"/work-orders").data


def test_provider_other_provider_and_org_isolation(client,data):
    row=order(data);act(data,row,"dispatched",dispatch_method="EMAIL")
    membership=core.CoreOrganizationMember.query.filter_by(user_id=data.users["resident"].id,organization_id=data.org.id).one()
    role=core.CoreRole.query.filter_by(slug="provider",organization_id=data.org.id).one()
    db.session.add(core.CoreRoleAssignment(organization_id=data.org.id,user_id=membership.user_id,role_id=role.id))
    db.session.commit();client.login("resident")
    assert client.get(f"{BASE}/work-orders/{row.id}").status_code==404
    assert row.reference.encode() not in client.get(BASE+"/work-orders").data
    client.login("outsider")
    assert client.get(f"/uip/other/work-orders/{row.id}").status_code==404
    assert client.get(f"{BASE}/work-orders/{row.id}").status_code==403


def test_staff_and_provider_post_routes_csrf_versions_permissions(client,data):
    row=order(data);db.session.commit()
    assert client.post(f"{BASE}/work-orders/{row.id}/actions",data={"action":"dispatched"}).status_code==400
    assert client.safe_post(f"{BASE}/work-orders/{row.id}/actions",dict(action="dispatched",expected_version=row.version,request_key=key(),dispatch_method="EMAIL")).status_code==302
    client.login("provider")
    assert client.safe_post(f"{BASE}/work-orders/{row.id}/actions",dict(action="accepted",expected_version=row.version,request_key=key())).status_code==302
    assert client.safe_post(f"{BASE}/work-orders/{row.id}/actions",dict(action="cancelled",expected_version=row.version,request_key=key(),note="No",reason_code="OTHER")).status_code==403
    task=operations.add_task(data.org.id,data.users["manager"].id,data.issue.id,"Internal")
    db.session.commit()
    assert client.safe_post(f"{BASE}/task/{task.id}/cancel",dict(reason="Forbidden",expected_version=task.uip_version)).status_code==403
    assert client.safe_post(f"{BASE}/task/{task.id}/complete",{}).status_code==403


def test_manager_register_templates_and_post_forms(client,data):
    assert client.get(BASE+"/providers").status_code==200
    assert client.get(BASE+"/providers/new").status_code==200
    assert client.safe_post(BASE+"/providers/new",dict(name="Web provider",availability="UNKNOWN",capabilities="SECURITY")).status_code==302
    p=uip.UipProvider.query.one()
    assert client.get(f"{BASE}/providers/{p.id}").status_code==200
    assert client.get(f"{BASE}/providers/{p.id}/edit").status_code==200
    assert client.get(BASE+"/work-orders").status_code==200
    client.login("receptionist")
    response=client.get(BASE+"/dashboard")
    assert response.status_code==200 and b"Open issues" in response.data and b"Log interaction" in response.data
    assert client.get(BASE+"/providers").status_code==403


def test_staff_order_template_and_metrics(client,data):
    from app.uip.services.dashboard import metrics
    p=provider(data)
    assert metrics(data.org.id,data.users["manager"].id)["Unassigned open issues"]==1
    row=order(data,p);db.session.commit()
    assert client.get(f"{BASE}/work-orders/{row.id}").status_code==200
    counts=metrics(data.org.id,data.users["manager"].id)
    assert counts["Created, not dispatched"]==1 and counts["Dispatched"]==0 and counts["Unassigned open issues"]==0
    assert b"Active work orders" in client.get(BASE+"/dashboard").data
    orders_page = client.get(BASE+"/work-orders").data
    assert row.reference.encode() in orders_page and b"Created" in orders_page


def test_provider_receives_verification_rejection_reason_only_on_its_order(client,data):
    from phase3_helpers import completed
    row=completed(data)
    act(data,row,"verification_rejected",note="Repair still leaking",reason_code="WORK_INCOMPLETE")
    db.session.commit();client.login("provider")
    response=client.get(f"{BASE}/work-orders/{row.id}")
    assert response.status_code==200 and b"Repair still leaking" in response.data


def test_task_cancellation_http_preserves_visible_history(client,data):
    task=operations.add_task(data.org.id,data.users["manager"].id,data.issue.id,"Historical title")
    db.session.commit()
    assert client.safe_post(f"{BASE}/task/{task.id}/cancel",dict(reason="Duplicate follow-up",expected_version=task.uip_version)).status_code==302
    response=client.get(BASE+"/interaction/MG-TEST")
    assert b"Historical title" in response.data and b"Duplicate follow-up" in response.data
