import pytest
import sqlalchemy as sa
from werkzeug.exceptions import NotFound, Forbidden
from bootstrap import db, core, uip, ROOT
from app.uip.services import audit, register
from test_register import make_member, make_property, MEMBER
BASE = "/uip/manor-gardens"


def test_same_transaction_rollback(data):
    count = core.CoreOrganizationMember.query.count()
    make_member(data)
    db.session.flush()
    assert uip.UipAuditEvent.query.count() == 1
    db.session.rollback()
    assert uip.UipAuditEvent.query.count() == 0
    assert uip.UipMemberProfile.query.count() == 0
    assert core.CoreOrganizationMember.query.count() == count


def test_audit_service_never_commits(data, monkeypatch):
    monkeypatch.setattr(db.session, "commit", lambda: pytest.fail("Independent commit"))
    make_member(data)
    assert uip.UipAuditEvent.query.count() == 1


def test_failed_audit_flush_rolls_back_operational_write(client, data, monkeypatch):
    original = audit.record
    def fail(*args, **kwargs):
        event = original(*args, **kwargs)
        event.actor_user_id = -1  # Force a real PostgreSQL FK failure at commit.
        return event
    monkeypatch.setattr(audit, "record", fail)
    count = core.CoreOrganizationMember.query.count()
    assert client.safe_post(BASE + "/members/new", MEMBER).status_code == 409
    assert uip.UipMemberProfile.query.count() == 0
    assert uip.UipAuditEvent.query.count() == 0
    assert core.CoreOrganizationMember.query.count() == count


@pytest.mark.parametrize("metadata", [{"email":"private@example.invalid"}, {"body":"private"}, {"password":"secret"}, {"changed_fields":["Private message"]}, {"changed_fields":"name"}, "not a dictionary"])
def test_metadata_allowlist(data, metadata):
    member=make_member(data)
    with pytest.raises(ValueError):
        audit.record(data.org.id, data.users["manager"].id, "member.updated", member, metadata)


def test_safe_metadata_only_field_names(data):
    make_member(data)
    event=uip.UipAuditEvent.query.one()
    assert set(event.metadata_json) == {"changed_fields"}
    assert set(event.metadata_json["changed_fields"]) <= audit.SAFE_FIELDS
    assert "Test Person" not in str(event.metadata_json)
    assert "person@example.invalid" not in str(event.metadata_json)


def test_org_scoped_history_and_single_event(client, data):
    local=make_member(data)
    register.save_member(data.other.id, data.outsider.id, {**MEMBER,"name":"Foreign secret"})
    db.session.commit()
    foreign=uip.UipAuditEvent.query.filter_by(organization_id=data.other.id).one()
    events=audit.events(data.org.id, data.users["manager"].id).all()
    assert len(events)==1 and events[0].entity_id==local.id
    assert client.get(BASE + f"/audit/{foreign.id}").status_code == 404
    assert client.get(BASE + f"/audit/{events[0].id}").status_code == 200
    with pytest.raises(NotFound):
        audit.record(data.other.id, data.outsider.id, "member.updated", local)


def test_shared_audit_stream_is_untouched(data, client):
    legacy=core.CoreAuditEvent(organization_id=None, action="SACE_SENTINEL", details="SACE private data")
    db.session.add(legacy);db.session.commit()
    make_member(data); db.session.commit()
    assert core.CoreAuditEvent.query.count() == 1
    assert core.CoreAuditEvent.query.one().details == "SACE private data"
    response=client.get(BASE + "/audit")
    assert b"SACE_SENTINEL" not in response.data and b"SACE private data" not in response.data
    assert [e.action for e in core.CoreAuditEvent.query.all()] == ["SACE_SENTINEL"]


def test_no_shared_audit_import_or_use_in_uip():
    paths=list((ROOT / "app/uip").rglob("*.py"))+[ROOT / "app/models/uip.py"]
    for path in paths:
        source=path.read_text(encoding="utf-8")
        assert "CoreAuditEvent" not in source and "core_audit_event" not in source
        assert "program_sace" not in source


@pytest.mark.parametrize("statement", ["UPDATE uip_audit_event SET action='tampered'", "DELETE FROM uip_audit_event"])
def test_audit_append_only_in_postgresql(data, statement):
    make_member(data);db.session.commit()
    with pytest.raises(sa.exc.DBAPIError, match="append-only"), db.session.begin_nested():
        db.session.execute(sa.text(statement))
    assert uip.UipAuditEvent.query.count()==1


def test_foreign_role_definition_does_not_authorize(data):
    assignment=core.CoreRoleAssignment.query.filter_by(user_id=data.users["manager"].id).one()
    assignment.role.organization_id=data.other.id;db.session.flush()
    with pytest.raises(Forbidden):
        audit.events(data.org.id, data.users["manager"].id)


def test_foreign_role_definition_denies_phase1_routes(client, data):
    assignment=core.CoreRoleAssignment.query.filter_by(user_id=data.users["manager"].id).one()
    assignment.role.organization_id=data.other.id;db.session.commit()
    assert client.get(BASE + "/dashboard").status_code == 403
    assert client.get(BASE + "/interaction/new").status_code == 403


def test_phase3_metadata_rejects_free_text_and_credentials(data):
    from phase3_helpers import order
    from app.uip.services import audit
    row=order(data)
    for metadata in ({"note":"private"},{"password":"secret"},{"reason_code":"free text"},{"new_state":"private message"},{"version":"secret"}):
        with pytest.raises(ValueError):
            audit.record(data.org.id,data.users["manager"].id,"work_order.created",row,metadata)
