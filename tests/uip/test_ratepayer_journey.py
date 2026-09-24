"""RP requests against disposable PostgreSQL; R2 transport is a deterministic fake.
Shared registration is an explicit integration boundary, not a second User model.
"""
from datetime import date, timedelta
from io import BytesIO
from types import SimpleNamespace
from urllib.parse import urlsplit, parse_qs
import pytest
import sqlalchemy as sa
from flask import g, request, redirect
from PIL import Image
from bootstrap import db, core, uip, AuthSubject
from test_register import make_member, make_property, import_records
from app.models.uip_operations import UipDocumentVersion

BASE = "/uip/manor-gardens"


def vault(data, email=None):
    member = make_member(data, email=email or data.users["resident"].email)
    prop = make_property(data, address="12 Municipal Road")
    import_records(data.org.id, data.users["manager"].id, "relationships", [dict(
        member_reference=member.reference, property_reference=prop.reference,
        relationship="owner", valid_from="2026-01-01", valid_to="", is_verified="true")])
    db.session.commit()
    return member, prop


def logout(client):
    with client.session_transaction() as session:
        session.clear()
    g.pop("_login_user", None)
    g.pop("csrf_token", None)


def snapshot():
    models = (uip.UipMemberProfile, uip.UipProperty, uip.UipPropertyMember,
              uip.UipRegisterImport, core.CoreOrganizationMember, core.CoreRoleAssignment)
    return {model.__name__: [tuple(getattr(row, c.name) for c in model.__table__.columns)
                            for row in model.query.order_by(model.id).all()] for model in models}


def test_no_vault_waiting_without_writes(client, data):
    client.login("resident")
    before = snapshot()
    count = core.CoreInteraction.query.count()
    statements = []
    connection = db.session.connection()
    def capture(conn, cursor, statement, *args): statements.append(statement)
    sa.event.listen(connection, "before_cursor_execute", capture)
    try:
        page = client.get(BASE + "/router?force=1")
        assert b"verify/ratepayer" in page.data
        response = client.get(BASE + "/verify/ratepayer")
        assert response.status_code == 200
        assert b"Ratepayer Waiting Room" in response.data
        assert b"does not mean you are not a Ratepayer" in response.data
        assert not any(s.lstrip().split()[0].upper() in {"INSERT", "UPDATE", "DELETE", "ALTER", "CREATE"} for s in statements)
    finally:
        sa.event.remove(connection, "before_cursor_execute", capture)
    assert snapshot() == before
    assert core.CoreInteraction.query.count() == count


def test_matched_vault_board_and_property_readonly(client, data):
    member, prop = vault(data)
    client.login("resident")
    before = snapshot()
    response = client.get(BASE + "/verify/ratepayer")
    assert response.status_code == 302 and response.location.endswith("/ratepayer-workspace")
    page = client.get(response.location)
    assert page.status_code == 200
    for text in (b"RP Board", b"12 Municipal Road", b"My Property", b"Mandates", b"Lodge a Query", b"My Queries"):
        assert text in page.data
    assert b"/edit" not in page.data and b"Manage Profile" not in page.data
    assert client.get(BASE + "/public-mandates").status_code == 200
    assert snapshot() == before


@pytest.mark.parametrize("invalid", ["unmatched", "inactive", "expired", "future", "manual", "unverified", "ambiguous", "foreign"])
def test_available_vault_without_current_match_denied_not_waiting(client, data, invalid):
    member, prop = vault(data)
    relation = uip.UipPropertyMember.query.one()
    if invalid == "unmatched": member.email = "another@example.invalid"
    elif invalid == "inactive": member.is_active = False
    elif invalid == "expired": relation.valid_to = date(2026, 1, 2)
    elif invalid == "future": relation.valid_from = date.today() + timedelta(days=1)
    elif invalid == "manual": member.record_source = "MANUAL"
    elif invalid == "unverified": relation.is_verified = False
    elif invalid == "ambiguous": make_member(data, reference="M2", email=member.email)
    elif invalid == "foreign": member.email = data.outsider.email
    db.session.commit()
    client.login("resident")
    before = snapshot()
    response = client.get(BASE + "/verify/ratepayer")
    assert response.status_code == 403
    assert b"Waiting Room" not in response.data
    assert client.get(BASE + "/ratepayer-workspace", follow_redirects=True).status_code == 403
    assert snapshot() == before
    assert not core.CoreInteraction.query.filter_by(interaction_type="ratepayer_claim").count()


def test_query_and_own_status(client, data):
    vault(data)
    client.login("resident")
    before = snapshot()
    response = client.safe_post(BASE + "/ratepayer-workspace", dict(title="Pothole", description="Near the corner"))
    assert response.status_code == 302
    row = core.CoreInteraction.query.filter_by(interaction_type="municipal_fault").one()
    assert row.creator_id == data.users["resident"].id and row.status == "NEW"
    row.status = "IN_PROGRESS"
    db.session.add(core.CoreInteraction(organization_id=data.org.id, creator_id=data.users["manager"].id,
        title="Other person's private query", interaction_type="municipal_fault"))
    db.session.commit()
    page = client.get(BASE + "/ratepayer-workspace")
    assert b"Pothole" in page.data and b"IN_PROGRESS" in page.data
    assert b"Other person" not in page.data
    assert snapshot() == before
    assert core.CoreAuditEvent.query.filter_by(action="RP_QUERY_CREATED", user_id=data.users["resident"].id).count() == 1


@pytest.fixture
def r2(monkeypatch):
    from app.utils import cloudflare_r2
    stored = {}
    for key, value in dict(R2_ENDPOINT_URL="https://r2.example.invalid", R2_ACCESS_KEY="test-key",
            R2_SECRET_KEY="test-secret", R2_BUCKET_NAME="test-bucket", R2_PUBLIC_DOMAIN="https://media.example.invalid").items():
        monkeypatch.setenv(key, value)
    def put_object(**values): stored[values["Key"]] = values
    def get_object(**values): return {"Body": BytesIO(stored[values["Key"]]["Body"])}
    monkeypatch.setattr(cloudflare_r2.boto3, "client", lambda *args, **kwargs: SimpleNamespace(put_object=put_object, get_object=get_object))
    return stored


def png():
    result = BytesIO()
    Image.new("RGB", (2, 2), "red").save(result, format="PNG")
    return result.getvalue()


def test_photo_r2_roundtrip_metadata_and_owner_guard(client, data, r2, monkeypatch):
    vault(data)
    client.login("resident")
    before = snapshot()
    from app.program_uip.services import documents
    monkeypatch.setattr(documents, "root", lambda *args: pytest.fail("Permanent document storage used"))
    content = png()
    response = client.safe_post(BASE + "/ratepayer-workspace", dict(title="Pothole photo", description="Road",
        photo=(BytesIO(content), "road.png")))
    assert response.status_code == 302
    document = uip.UipDocument.query.filter_by(category="RP_QUERY_PHOTO").one()
    version = UipDocumentVersion.query.filter_by(document_id=document.id).one()
    assert version.storage_key.startswith("uip/rp_queries/") and len(version.storage_key) <= 64
    assert r2[version.storage_key]["Body"] == content
    assert r2[version.storage_key]["Bucket"] == "test-bucket"
    assert version.size_bytes == len(content) and version.content_type == "image/png"
    path = BASE + f"/ratepayer-workspace/photos/{document.id}"
    result = client.get(path)
    assert result.status_code == 200 and result.data == content
    assert result.headers["Cache-Control"] == "private, no-store"
    assert snapshot() == before
    client.login("manager")
    assert client.get(path).status_code == 403
    client.login("outsider")
    assert client.get(path).status_code == 403


@pytest.mark.parametrize("failure", ["invalid", "too_large", "r2_unavailable"])
def test_photo_failure_no_query_or_metadata(client, data, monkeypatch, r2, failure):
    vault(data)
    client.login("resident")
    content = png()
    if failure == "invalid": content = b"not a photo"
    if failure == "too_large": content = b"x" * (5 * 1024 * 1024 + 1)
    if failure == "r2_unavailable":
        from app.utils import cloudflare_r2
        def unavailable(*args, **kwargs): raise RuntimeError("SECRET_PROVIDER_DETAIL")
        monkeypatch.setattr(cloudflare_r2, "upload_file_to_r2", unavailable)
    response = client.safe_post(BASE + "/ratepayer-workspace", dict(title="Fault", description="Details",
        photo=(BytesIO(content), "road.png")))
    assert response.status_code == (503 if failure == "r2_unavailable" else 400)
    assert b"SECRET_PROVIDER_DETAIL" not in response.data
    assert core.CoreInteraction.query.filter_by(interaction_type="municipal_fault").count() == 0
    assert uip.UipDocument.query.filter_by(category="RP_QUERY_PHOTO").count() == 0


@pytest.mark.parametrize("role", ["manager", "committee_member", "owner", "resident", "receptionist", "provider"])
def test_browser_vault_writes_blocked_all_current_roles(client, data, role):
    member, prop = vault(data)
    client.login(role)
    before = snapshot()
    for path in (f"/members/{member.id}/edit", f"/properties/{prop.id}/edit", "/register/import"):
        assert client.get(BASE + path).status_code == 403
        assert client.safe_post(BASE + path, {}).status_code == 403
    for path in (f"/members/{member.id}/representatives", f"/properties/{prop.id}/members"):
        assert client.safe_post(BASE + path, {}).status_code == 403
    assert client.get(BASE + "/vault-check").status_code == 403
    assert client.safe_post(BASE + "/members/new", {}).status_code == 404
    assert snapshot() == before


def test_frontdoor_registration_handoff_and_role_selection(app, client, data):
    # Shared auth remains unchanged. Capture its next URL, then simulate its authenticated return.
    app.add_url_rule("/auth/register", endpoint="auth_bp.register", view_func=lambda: "Registration boundary")
    logout(client)
    assert client.get("/uip/").status_code == 200
    about = client.get(BASE + "/about")
    assert about.status_code == 200 and b"Register" in about.data
    response = client.safe_post("/uip/select", {"org_slug": data.org.slug})
    assert response.status_code == 302 and urlsplit(response.location).path == "/auth/register"
    next_url = parse_qs(urlsplit(response.location).query)["next"][0]
    assert next_url == BASE + "/router?force=1"
    client.login("resident")
    page = client.get(next_url)
    assert page.status_code == 200 and b"verify/ratepayer" in page.data
    assert b"Waiting Room" in client.get(BASE + "/verify/ratepayer").data


@pytest.mark.parametrize("status", ["active", "complimentary", "suspended"])
def test_rp_board_preserves_organization_entitlement(client, data, status):
    vault(data)
    subject = AuthSubject(slug="uip")
    db.session.add(subject)
    db.session.flush()
    db.session.add(core.CoreOrganizationEntitlement(organization_id=data.org.id,
        subject_id=subject.id, status=status))
    db.session.commit()
    client.login("resident")
    response = client.get(BASE + "/ratepayer-workspace")
    if status == "suspended":
        assert response.status_code == 302 and response.location.endswith("/service-status")
    else:
        assert response.status_code == 200 and b"RP Board" in response.data


def test_other_verified_rp_cannot_read_photo(client, data, r2):
    vault(data)
    client.login("resident")
    assert client.safe_post(BASE + "/ratepayer-workspace", dict(title="Photo", description="Location",
        photo=(BytesIO(png()), "road.png"))).status_code == 302
    document = uip.UipDocument.query.filter_by(category="RP_QUERY_PHOTO").one()
    other_member = make_member(data, reference="M2", email=data.users["manager"].email)
    other_property = make_property(data, reference="P2")
    import_records(data.org.id, data.users["manager"].id, "relationships", [dict(
        member_reference="M2", property_reference="P2", relationship="owner", is_verified="true")])
    db.session.commit()
    client.login("manager")
    assert client.get(BASE + "/ratepayer-workspace").status_code == 200
    assert client.get(BASE + f"/ratepayer-workspace/photos/{document.id}").status_code == 404
    assert b"View photograph" not in client.get(BASE + "/ratepayer-workspace").data


def test_r2_existing_organogram_caller_contract(r2):
    from app.utils.cloudflare_r2 import upload_file_to_r2
    from werkzeug.datastructures import FileStorage
    result = upload_file_to_r2(FileStorage(stream=BytesIO(png()), filename="photo.png", content_type="image/png"))
    assert result.startswith("https://media.example.invalid/organogram/")
    assert next(iter(r2)).startswith("organogram/")


def test_shared_registration_decision_retains_uip_next(app, client, data, monkeypatch):
    # Execute the existing decision function without importing the unsafe shared factory.
    # Account creation/pledge/password forms are outside this isolated UIP fixture.
    import ast
    import sys
    from bootstrap import ROOT, User
    from flask import session, flash, url_for
    monkeypatch.setattr(sys.modules["app.models"], "User", User, raising=False)
    tree = ast.parse((ROOT / "app/auth/routes.py").read_text(encoding="utf-8"))
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "register_decision")
    fn.decorator_list = []
    enrolled = []
    scope = dict(session=session, request=request, db=db, redirect=redirect, flash=flash, url_for=url_for,
        _ensure_enrollment_row=lambda **kw: enrolled.append(kw))
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "<real register_decision>", "exec"), scope)
    app.add_url_rule("/auth/register/decision", view_func=scope["register_decision"])
    client.login("resident")
    with client.session_transaction() as session:
        session["reg_ctx"] = dict(subject="uip", next_url=BASE + "/router?force=1")
    response = client.get("/auth/register/decision")
    assert response.status_code == 302 and response.location == BASE + "/router?force=1"
    assert enrolled == [dict(user_id=data.users["resident"].id, subject_slug="uip")]
    assert client.get(response.location).status_code == 200
