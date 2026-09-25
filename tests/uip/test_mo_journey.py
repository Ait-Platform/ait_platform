"""Real MO HTTP boundaries; reuse the outer disposable UIP session."""
import csv
import io
import re
from datetime import date
from uuid import uuid4
from hashlib import sha256
import pytest
from bootstrap import db, core, uip
from app.models.uip import UipDocumentVersion
from app.models.uip_governance import UipDelegation
from app.program_uip.completion_routes import CSV_COLUMNS

BASE = "/uip/manor-gardens"
CONTENT = b"synthetic R2 evidence bytes"


@pytest.fixture
def evidence(data):
    def create(org, user, category="RP_QUERY_PHOTO", referred=True):
        issue = core.CoreInteraction(organization_id=org.id, creator_id=user.id,
            title="Referred query", interaction_type="municipal_fault", status="NEW", reference=uuid4().hex)
        db.session.add(issue); db.session.flush()
        referral = uip.UipMunicipalReferral(organization_id=org.id, interaction_id=issue.id, status="ESCALATED_TO_MO") if referred else None
        if referral: db.session.add(referral)
        document = uip.UipDocument(organization_id=org.id, uploader_id=user.id,
            interaction_id=issue.id, filename="query.png", file_type="png", category=category,
            access_classification="PRIVATE", current_version=1)
        db.session.add(document); db.session.flush()
        version = UipDocumentVersion(organization_id=org.id, document_id=document.id, version=1,
            storage_key="uip/rp_queries/" + uuid4().hex + ".png", size_bytes=len(CONTENT),
            content_type="image/png", effective_date=date.today(), actor_user_id=user.id,
            filename="query.png", sha256=sha256(CONTENT).hexdigest(), replacement_reason="Initial photo")
        db.session.add(version); db.session.commit()
        return document, referral
    return create


def download(org, document):
    return f"/uip/{org.slug}/operations/documents/{document.id}/versions/1/download"


def test_mo_board_access_and_non_mo_blocked(client, data):
    client.login("resident")
    assert client.get(BASE + "/mo-dashboard").status_code == 403
    assert client.safe_post(BASE + "/mo-action/999", {"action":"acknowledge"}).status_code == 403
    client.login("municipal_officer")
    assert client.get(BASE + "/mo-dashboard").status_code == 200


def test_mo_vault_import_http(client, data, monkeypatch):
    from app.program_uip.services import register
    calls = []
    original = register.process_import_batch
    def observe(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(args)
        return result
    monkeypatch.setattr(register, "process_import_batch", observe)
    client.login("municipal_officer")
    assert client.get(BASE + "/mo-vault/import").status_code == 200
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=CSV_COLUMNS["members"])
    writer.writeheader()
    writer.writerow(dict(reference="MO-1", name="Test RP", member_type="person",
        email="rp@example.invalid", phone="", is_active="true", eligibility_status="eligible"))
    content = stream.getvalue().encode()
    def payload(**extra):
        return dict(kind="members", source_identifier="Municipal supplied register", batch_reference="MO-B1",
            effective_date="2026-09-01", file=(io.BytesIO(content), "register.csv"), **extra)
    preview = client.safe_post(BASE + "/mo-vault/import", payload(operation="preview"))
    assert preview.status_code == 200, preview.get_data(as_text=True)[-5000:]
    token = re.search(rb'name="preview_token"[^>]*value="([^"]+)"', preview.data)
    assert token and calls
    assert not uip.UipRegisterImport.query.count()
    result = client.safe_post(BASE + "/mo-vault/import", payload(operation="commit", preview_token=token.group(1).decode()))
    assert result.status_code == 302
    batch = uip.UipRegisterImport.query.one()
    member = uip.UipMemberProfile.query.filter_by(reference="MO-1").one()
    assert batch.source_type == "MUNICIPAL" and batch.status == "COMPLETED"
    assert batch.imported_by_user_id == data.users["municipal_officer"].id
    assert batch.batch_reference == "MO-B1" and member.last_import_id == batch.id
    assert member.record_source == "MUNICIPAL" and len(calls) == 2


def test_mo_legitimately_referred_r2_photo_http(client, data, evidence, monkeypatch):
    from app.utils import cloudflare_r2
    document, referral = evidence(data.org, data.users["resident"])
    calls = []
    def read(key):
        calls.append(key)
        return CONTENT
    monkeypatch.setattr(cloudflare_r2, "read_file_from_r2", read)
    client.login("municipal_officer")
    page = client.get(BASE + "/mo-dashboard")
    assert page.status_code == 200 and b"fa-camera" in page.data
    response = client.get(download(data.org, document))
    assert response.status_code == 200
    assert response.data == CONTENT and len(calls) == 1
    assert calls[0].startswith("uip/rp_queries/")


@pytest.mark.parametrize("case", ["private", "unreferred", "foreign"])
def test_mo_denied_unrelated_evidence_http(client, data, evidence, monkeypatch, case):
    from app.utils import cloudflare_r2
    monkeypatch.setattr(cloudflare_r2, "read_file_from_r2", lambda key: pytest.fail("Denied evidence read from R2"))
    org = data.other if case == "foreign" else data.org
    document, _ = evidence(org, data.users["resident"],
        category="PRIVATE_RECORD" if case == "private" else "RP_QUERY_PHOTO", referred=case != "unreferred")
    client.login("municipal_officer")
    assert client.get(download(org, document)).status_code in (403, 404)
    if case == "foreign":
        assert client.get(download(data.org, document)).status_code in (403, 404)


def test_mo_status_actions_and_other_org_mo_denied_http(client, data, evidence):
    document, referral = evidence(data.org, data.users["resident"])
    client.login("municipal_officer")
    for action, status in (("acknowledge","ACKNOWLEDGED"), ("dispatch","DISPATCHED"), ("resolve","RESOLVED")):
        assert client.safe_post(BASE + f"/mo-action/{referral.id}", {"action":action}).status_code == 302
        assert db.session.get(uip.UipMunicipalReferral, referral.id).status == status
    role = core.CoreRole(organization_id=data.other.id, slug="municipal_officer", name="Other MO")
    db.session.add(role); db.session.flush()
    core.CoreRoleAssignment.query.filter_by(user_id=data.outsider.id).delete()
    db.session.add(core.CoreRoleAssignment(organization_id=data.other.id, user_id=data.outsider.id, role_id=role.id))
    db.session.commit()
    client.login("outsider")
    assert client.get("/uip/other/mo-dashboard").status_code == 200
    assert client.safe_post(BASE + f"/mo-action/{referral.id}", {"action":"acknowledge"}).status_code in (403,404)
    assert referral.status == "RESOLVED"


def test_mo_cannot_manually_edit_vault_or_gain_synthetic_authority(client, data):
    client.login("municipal_officer")
    before = core.CoreRoleAssignment.query.count(), UipDelegation.query.count()
    for path in ("/members/999/edit", "/properties/999/edit", "/properties/999/members"):
        assert client.safe_post(BASE + path, {}).status_code in (403,404)
    assert client.safe_post(BASE + "/members/new", {}).status_code == 404
    assert client.get(BASE + "/mo-dashboard").status_code == 200
    assert before == (core.CoreRoleAssignment.query.count(), UipDelegation.query.count())
    roles = core.CoreRoleAssignment.query.filter_by(user_id=data.users["municipal_officer"].id).all()
    assert [r.role.slug for r in roles] == ["municipal_officer"]


def test_selection_only_claim_then_existing_resolution_assignment(client, data):
    from app.program_uip.committee_routes import execute_resolution_adoption
    client.login("resident")
    before = core.CoreRoleAssignment.query.filter_by(user_id=data.users["resident"].id).count()
    assert client.get(BASE + "/verify/mo").status_code == 302
    assert core.CoreRoleAssignment.query.filter_by(user_id=data.users["resident"].id).count() == before
    assert client.get(BASE + "/mo-dashboard").status_code == 403
    claim = core.CoreInteraction.query.filter_by(creator_id=data.users["resident"].id, interaction_type="mo_claim").one()
    from datetime import datetime
    meeting = uip.UipCommitteeMeeting(organization_id=data.org.id, title="MO authority", scheduled_at=datetime.now(), status="CONCLUDED")
    db.session.add(meeting); db.session.flush()
    resolution = uip.UipResolution(organization_id=data.org.id, meeting_id=meeting.id, title="MO assignment",
        status="ADOPTED", result_basis={"type":"access_bundle", "interaction_ids":[claim.id]})
    db.session.add(resolution); db.session.flush()
    execute_resolution_adoption(data.org, resolution, db)
    db.session.commit()
    assert resolution.status == "ADOPTED" and claim.status == "VERIFIED"
    assert client.get(BASE + "/mo-dashboard").status_code == 200
    assert client.get(BASE + "/mo-vault/import").status_code == 200


def test_manager_cannot_import_authoritative_vault(client, data):
    from app.models.uip import UipMemberProfile
    client.login("manager")
    assert client.get(BASE + "/mo-vault/import").status_code == 403
    assert client.safe_post(BASE + "/mo-vault/import", {}).status_code == 403
    
    # Manager CAN use /register/import, but it must be MANUAL, not MUNICIPAL
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=CSV_COLUMNS["members"])
    writer.writeheader()
    writer.writerow(dict(reference="MGR-1", name="Manager RP", member_type="person",
        email="mgr@example.invalid", phone="", is_active="true", eligibility_status="eligible"))
    content = stream.getvalue().encode()
    def payload(**extra):
        return dict(kind="members", source_identifier="Manager", batch_reference="B1",
            effective_date="2026-09-01", file=(io.BytesIO(content), "register.csv"), **extra)
    
    preview = client.safe_post(BASE + "/register/import", payload(operation="preview"))
    assert preview.status_code == 200
    token = re.search(rb'name="preview_token"[^>]*value="([^"]+)"', preview.data)
    assert token
    
    result = client.safe_post(BASE + "/register/import", payload(operation="commit", preview_token=token.group(1).decode()))
    assert result.status_code == 302
    
    # Prove it's MANUAL and NOT authoritative
    member = UipMemberProfile.query.filter_by(reference="MGR-1").one()
    assert member.record_source == "MANUAL"
    
    # Ratepayer verification uses is_verified which requires MUNICIPAL for trust, etc.
    # Actually, we don't need to explicitly verify the verification function here if the record_source is proven MANUAL.

def test_ratepayer_admin_cannot_import_authoritative_vault(client, data):
    from app.models.uip import UipMemberProfile
    from app.models.core import CoreOrganizationMember, CoreRole, CoreRoleAssignment
    from app.models.auth import User
    from app.extensions import db
    import io, csv, re
    
    ra = User(name="ra_admin", email="ra@example.invalid", is_active=1)
    db.session.add(ra)
    db.session.flush()
    mem = CoreOrganizationMember(organization_id=data.org.id, user_id=ra.id, is_active=1)
    db.session.add(mem)
    r = CoreRole.query.filter_by(slug="RATEPAYER_ADMIN", organization_id=data.org.id).first()
    if not r:
        r = CoreRole(name="RATEPAYER_ADMIN", slug="RATEPAYER_ADMIN", organization_id=data.org.id)
        db.session.add(r)
    db.session.flush()
    db.session.add(CoreRoleAssignment(user_id=ra.id, organization_id=data.org.id, role_id=r.id))
    db.session.commit()
    
    with client.session_transaction() as session:
        session.clear()
        session["_user_id"] = str(ra.id)
        session["_fresh"] = True
    
    assert client.get(BASE + "/mo-vault/import").status_code == 403
    
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=CSV_COLUMNS["members"])
    writer.writeheader()
    writer.writerow(dict(reference="RA-1", name="RA RP", member_type="person",
        email="ra@example.invalid", phone="", is_active="true", eligibility_status="eligible"))
    content = stream.getvalue().encode()
    def payload(**extra):
        return dict(kind="members", source_identifier="RA", batch_reference="B2",
            effective_date="2026-09-01", file=(io.BytesIO(content), "register.csv"), **extra)
    
    preview = client.safe_post(BASE + "/register/import", payload(operation="preview"))
    token = re.search(rb'name="preview_token"[^>]*value="([^"]+)"', preview.data)
    client.safe_post(BASE + "/register/import", payload(operation="commit", preview_token=token.group(1).decode()))
    
    member = UipMemberProfile.query.filter_by(reference="RA-1").one()
    assert member.record_source == "MANUAL"

def test_existing_mo_vault_records_cannot_be_overwritten_by_manager(client, data):
    from app.models.uip import UipMemberProfile, UipRegisterImportException
    # 1. MO creates MUNICIPAL record
    client.login("municipal_officer")
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=CSV_COLUMNS["members"])
    writer.writeheader()
    writer.writerow(dict(reference="MO-OVR-1", name="Original MO", member_type="person",
        email="", phone="", is_active="true", eligibility_status="eligible"))
    content = stream.getvalue().encode()
    def payload(**extra):
        return dict(kind="members", source_identifier="MO", batch_reference="B1",
            effective_date="2026-09-01", file=(io.BytesIO(content), "register.csv"), **extra)
    preview = client.safe_post(BASE + "/mo-vault/import", payload(operation="preview"))
    token = re.search(rb'name="preview_token"[^>]*value="([^"]+)"', preview.data)
    client.safe_post(BASE + "/mo-vault/import", payload(operation="commit", preview_token=token.group(1).decode()))
    member = UipMemberProfile.query.filter_by(reference="MO-OVR-1").one()
    assert member.record_source == "MUNICIPAL"
    assert member.name == "Original MO"
    
    # 2. Manager tries to overwrite
    client.login("manager")
    stream2 = io.StringIO()
    writer2 = csv.DictWriter(stream2, fieldnames=CSV_COLUMNS["members"])
    writer2.writeheader()
    writer2.writerow(dict(reference="MO-OVR-1", name="Hacked Manager", member_type="person",
        email="", phone="", is_active="false", eligibility_status="eligible"))
    content2 = stream2.getvalue().encode()
    def payload2(**extra):
        return dict(kind="members", source_identifier="Manager", batch_reference="B2",
            effective_date="2026-09-02", file=(io.BytesIO(content2), "register.csv"), **extra)
    
    preview2 = client.safe_post(BASE + "/register/import", payload2(operation="preview"))
    token2 = re.search(rb'name="preview_token"[^>]*value="([^"]+)"', preview2.data)
    result = client.safe_post(BASE + "/register/import", payload2(operation="commit", preview_token=token2.group(1).decode()))
    
    # 3. Prove it failed (UipRegisterImportException created) and record unchanged
    member = UipMemberProfile.query.filter_by(reference="MO-OVR-1").one()
    assert member.name == "Original MO"
    assert member.is_active is True
    assert UipRegisterImportException.query.filter_by(source_reference="MO-OVR-1").count() > 0





