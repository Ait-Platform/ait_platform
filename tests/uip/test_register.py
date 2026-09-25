import pytest
import sqlalchemy as sa
from werkzeug.exceptions import Forbidden, NotFound, BadRequest
from bootstrap import db, core, uip
from app.program_uip.services import register, audit
BASE = "/uip/manor-gardens"
MEMBER = dict(reference="M1", name="Test Person", member_type="person", email="person@example.invalid", phone="", is_active="true", eligibility_status="unverified")
PROPERTY = dict(reference="P1", address="Test address", classification="residential", rates_reference="", is_active="true")


def import_records(organization_id, actor_user_id, kind, rows, effective_date=None):
    from datetime import date
    batch, summary = register.process_import_batch(organization_id, actor_user_id, kind, rows,
        dict(source_identifier="Synthetic municipal register", batch_reference="test-fixture",
             date_received=date(2026,1,1), effective_date=effective_date or date(2026,1,1)), is_authoritative=True)
    if summary["exceptions"] > 0:
        from app.models.uip import UipRegisterImportException
        exs = UipRegisterImportException.query.filter_by(import_id=batch.id).all()
        for e in exs: print("EXCEPTION:", e.__dict__)
    assert summary["exceptions"] == 0, (batch.status, summary)
    return batch


def import_member(organization_id, actor_user_id, row):
    import_records(organization_id, actor_user_id, "members", [row])
    return uip.UipMemberProfile.query.filter_by(organization_id=organization_id, reference=row["reference"]).one()


def import_property(organization_id, actor_user_id, row):
    import_records(organization_id, actor_user_id, "properties", [row])
    return uip.UipProperty.query.filter_by(organization_id=organization_id, reference=row["reference"]).one()


def make_member(data, **values):
    row = {**MEMBER, **values}
    import_records(data.org.id, data.users["manager"].id, "members", [row])
    return uip.UipMemberProfile.query.filter_by(organization_id=data.org.id, reference=row["reference"]).one()


def make_property(data, **values):
    row = {**PROPERTY, **values}
    import_records(data.org.id, data.users["manager"].id, "properties", [row])
    return uip.UipProperty.query.filter_by(organization_id=data.org.id, reference=row["reference"]).one()


def test_nonlogin_member_no_account_or_roles(data):
    users = db.session.query(core.CoreRoleAssignment).count()
    member = make_member(data)
    db.session.commit()
    assert member.membership.user_id is None
    assert member.eligibility_status == "unverified"
    assert core.CoreRoleAssignment.query.count() == users
    assert audit.events(data.org.id, data.users["manager"].id).one().action == "member.created"


def test_explicit_existing_membership_unchanged(data):
    membership = core.CoreOrganizationMember.query.filter_by(user_id=data.users["resident"].id).one()
    member = make_member(data, membership_id=str(membership.id))
    register.save_member(data.org.id, data.users["manager"].id, {**MEMBER, "is_active":"false"}, member.id)
    db.session.commit()
    assert member.membership_id == membership.id and membership.is_active


def test_cross_org_account_rejected(data):
    from datetime import date
    membership = core.CoreOrganizationMember.query.filter_by(user_id=data.outsider.id).one()
    batch, summary = register.process_import_batch(data.org.id,data.users["manager"].id,"members",
        [{**MEMBER,"membership_id":str(membership.id)}],dict(date_received=date(2026,1,1),effective_date=date(2026,1,1)))
    assert summary["exceptions"]==1 and summary["created"]==0
    assert batch.status=="WITH_EXCEPTIONS"
    assert uip.UipMemberProfile.query.count()==0


def test_member_property_pages_and_edit(client, data):
    member=make_member(data);item=make_property(data);db.session.commit()
    for path in ("/members", f"/members/{member.id}", f"/members/{member.id}/edit", "/properties", f"/properties/{item.id}", f"/properties/{item.id}/edit", "/audit"):
        resp = client.get(BASE+path)
        assert resp.status_code==200, f"{path}: {resp.get_data(as_text=True)}"
    # Authoritative names/addresses remain sealed; contact edits remain allowed.
    assert client.safe_post(BASE+f"/members/{member.id}/edit",{**MEMBER,"name":"Tampered"}).status_code==403
    assert client.safe_post(BASE+f"/properties/{item.id}/edit",{**PROPERTY,"address":"Tampered"}).status_code==403
    assert client.safe_post(BASE+f"/members/{member.id}/edit",{**MEMBER,"email":"updated@example.invalid"}).status_code==302
    assert member.name==MEMBER["name"] and item.address==PROPERTY["address"]
    assert member.email=="updated@example.invalid"


@pytest.mark.parametrize("role", ["resident", "provider", "outsider"])
def test_register_roles_denied(client, role):
    client.login(role)
    assert client.get(BASE + "/members").status_code == 403
    assert client.get(BASE + "/register/import").status_code == 403


def test_reception_read_not_edit(client, data):
    client.login("receptionist")
    assert client.get(BASE + "/members").status_code == 200
    assert client.get(BASE + "/register/import").status_code == 403
    assert client.get(BASE + "/audit").status_code == 403


def test_csrf_all_new_mutation_routes(client, data):
    member=make_member(data); item=make_property(data); db.session.commit()
    for path in (f"/members/{member.id}/edit", f"/members/{member.id}/preferences", f"/members/{member.id}/representatives", f"/properties/{item.id}/edit", f"/properties/{item.id}/members"):
        assert client.post(BASE + path, data=MEMBER).status_code == 400


def test_relationships_and_preferences(client, data):
    member=make_member(data); rep=make_member(data, reference="M2"); item=make_property(data); db.session.commit()
    dated=dict(valid_from="2026-01-01", valid_to="", is_verified="false")
    import_records(data.org.id,data.users["manager"].id,"relationships",[dict(member_reference=member.reference,property_reference=item.reference,relationship="owner",is_verified="false")])
    assert client.safe_post(BASE + f"/members/{member.id}/representatives", dict(**dated, representative_id=rep.id)).status_code == 302
    assert client.safe_post(BASE + f"/members/{member.id}/preferences", dict(channel="Email", preference="declined")).status_code == 302
    link=uip.UipPropertyMember.query.one()
    assert client.safe_post(BASE + f"/properties/{item.id}/members/{link.id}", dict(**{**dated, "valid_to":"2026-08-01"}, member_id=member.id, relationship="owner")).status_code == 403
    assert link.valid_to is None
    assert member.eligibility_status == "unverified"
    assert uip.UipCommunicationPreference.query.one().preference == "declined"
    assert client.get(BASE + f"/members/{member.id}").status_code == 200
    assert client.get(BASE + f"/properties/{item.id}").status_code == 200


def test_invalid_dates_and_self_representation(data):
    member=make_member(data)
    with pytest.raises(BadRequest):
        register.save_relationship(data.org.id, data.users["manager"].id, dict(valid_from="2026-02-01", valid_to="2026-01-01", is_verified="false", representative_id=member.id), member_id=member.id)
    with pytest.raises(BadRequest):
        register.save_relationship(data.org.id, data.users["manager"].id, dict(valid_from="2026-01-01", is_verified="false", representative_id=member.id), member_id=member.id)


def test_intake_links_and_staff_attribution(client, data):
    member=make_member(data); item=make_property(data); db.session.commit()
    payload=dict(title="Linked issue", description="Private message", category="SECURITY", channel="Telephone", priority="NORMAL", member_id=member.id, property_id=item.id)
    assert client.safe_post(BASE + "/interaction/new", payload).status_code == 302
    issue=core.CoreInteraction.query.filter_by(title="Linked issue").one()
    assert (issue.member_id, issue.property_id, issue.recorded_by) == (member.id, item.id, data.users["manager"].id)
    assert client.get(BASE + "/interaction/" + issue.reference).status_code == 200
    assert "Private message" not in str(uip.UipAuditEvent.query.all()[0].metadata_json)


def test_cross_org_links_service_and_database(data):
    member=make_member(data); item=make_property(data)
    with pytest.raises(NotFound):
        register.intake_links(data.other.id, data.outsider.id, member.id, item.id)
    db.session.flush()
    with pytest.raises(sa.exc.IntegrityError), db.session.begin_nested():
        db.session.add(uip.UipPropertyMember(organization_id=data.other.id, property_id=item.id, member_id=member.id, relationship="owner", valid_from=__import__('datetime').date(2026,1,1), is_verified=False))
        db.session.flush()


def test_cross_org_detail_and_edit(client, data):
    member=import_member(data.other.id, data.outsider.id, MEMBER)
    item=import_property(data.other.id, data.outsider.id, PROPERTY); db.session.commit()
    assert client.get(BASE + f"/members/{member.id}").status_code == 404
    assert client.safe_post(BASE + f"/properties/{item.id}/edit", PROPERTY).status_code == 404


def test_inactive_intake_rejected(data):
    member=make_member(data, is_active="false")
    with pytest.raises(BadRequest):
        register.intake_links(data.org.id, data.users["manager"].id, member.id)


def test_import_same_reference_is_unchanged(data):
    member=make_member(data);db.session.commit()
    count=core.CoreOrganizationMember.query.count()
    import_records(data.org.id,data.users["manager"].id,"members",[MEMBER.copy()])
    assert uip.UipMemberProfile.query.one().id==member.id
    assert core.CoreOrganizationMember.query.count()==count


def test_cross_org_interaction_fk_rejected(data):
    member=make_member(data);item=make_property(data);db.session.flush()
    with pytest.raises(sa.exc.IntegrityError), db.session.begin_nested():
        data.foreign.member_id=member.id
        data.foreign.property_id=item.id
        db.session.flush()


def test_cross_org_preference_and_representation_rejected(data):
    member=make_member(data)
    foreign=import_member(data.other.id,data.outsider.id,{**MEMBER,"reference":"FOREIGN"})
    with pytest.raises(NotFound):
        register.set_preference(data.org.id,data.users["manager"].id,foreign.id,dict(channel="Email",preference="allowed"))
    with pytest.raises(NotFound):
        register.save_relationship(data.org.id,data.users["manager"].id,dict(representative_id=foreign.id,valid_from="2026-01-01",is_verified="false"),member_id=member.id)


def test_cross_org_profile_membership_fk_rejected(data):
    membership=core.CoreOrganizationMember.query.filter_by(user_id=data.outsider.id).one()
    with pytest.raises(sa.exc.IntegrityError), db.session.begin_nested():
        db.session.add(uip.UipMemberProfile(organization_id=data.org.id,membership_id=membership.id,reference="X",name="X"))
        db.session.flush()


def test_removed_creation_routes_and_current_import_link(client):
    for path in ("members/new","properties/new"):
        assert client.get(BASE+"/"+path).status_code==404
        assert client.safe_post(BASE+"/"+path,{}).status_code==404
    for path in ("members","properties"):
        response=client.get(BASE+"/"+path)
        assert response.status_code==200
        assert (BASE+"/register/import").encode() in response.data







