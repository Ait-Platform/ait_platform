import pytest
import sqlalchemy as sa
from werkzeug.exceptions import Forbidden, NotFound, BadRequest
from bootstrap import db, core, uip
from app.uip.services import register, audit
BASE = "/uip/manor-gardens"
MEMBER = dict(reference="M1", name="Test Person", member_type="person", email="person@example.invalid", phone="", is_active="true", eligibility_status="unverified")
PROPERTY = dict(reference="P1", address="Test address", classification="residential", rates_reference="", is_active="true")


def make_member(data, **values):
    return register.save_member(data.org.id, data.users["manager"].id, {**MEMBER, **values})


def make_property(data, **values):
    return register.save_property(data.org.id, data.users["manager"].id, {**PROPERTY, **values})


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
    membership = core.CoreOrganizationMember.query.filter_by(user_id=data.outsider.id).one()
    with pytest.raises(NotFound):
        make_member(data, membership_id=str(membership.id))


def test_member_property_pages_and_edit(client, data):
    assert client.safe_post(BASE + "/members/new", MEMBER).status_code == 302
    assert client.safe_post(BASE + "/properties/new", PROPERTY).status_code == 302
    member = uip.UipMemberProfile.query.one(); item = uip.UipProperty.query.one()
    for path in ("/members", "/members/new", f"/members/{member.id}", f"/members/{member.id}/edit", "/properties", "/properties/new", f"/properties/{item.id}", f"/properties/{item.id}/edit", "/audit"):
        assert client.get(BASE + path).status_code == 200, path
    assert client.safe_post(BASE + f"/members/{member.id}/edit", {**MEMBER, "name":"Updated"}).status_code == 302
    assert member.name == "Updated"
    assert client.safe_post(BASE + f"/properties/{item.id}/edit", {**PROPERTY, "address":"Updated"}).status_code == 302
    assert item.address == "Updated"


@pytest.mark.parametrize("role", ["resident", "provider", "outsider"])
def test_register_roles_denied(client, role):
    client.login(role)
    assert client.get(BASE + "/members").status_code == 403
    assert client.safe_post(BASE + "/properties/new", PROPERTY).status_code == 403


def test_reception_read_not_edit(client, data):
    client.login("receptionist")
    assert client.get(BASE + "/members").status_code == 200
    assert client.safe_post(BASE + "/members/new", MEMBER).status_code == 403
    assert client.get(BASE + "/audit").status_code == 403


def test_csrf_all_new_mutation_routes(client, data):
    member=make_member(data); item=make_property(data); db.session.commit()
    for path in ("/members/new", f"/members/{member.id}/edit", f"/members/{member.id}/preferences", f"/members/{member.id}/representatives", "/properties/new", f"/properties/{item.id}/edit", f"/properties/{item.id}/members"):
        assert client.post(BASE + path, data=MEMBER).status_code == 400


def test_relationships_and_preferences(client, data):
    member=make_member(data); rep=make_member(data, reference="M2"); item=make_property(data); db.session.commit()
    dated=dict(valid_from="2026-01-01", valid_to="", is_verified="false")
    assert client.safe_post(BASE + f"/properties/{item.id}/members", dict(**dated, member_id=member.id, relationship="owner")).status_code == 302
    assert client.safe_post(BASE + f"/members/{member.id}/representatives", dict(**dated, representative_id=rep.id)).status_code == 302
    assert client.safe_post(BASE + f"/members/{member.id}/preferences", dict(channel="Email", preference="declined")).status_code == 302
    link=uip.UipPropertyMember.query.one()
    assert client.safe_post(BASE + f"/properties/{item.id}/members/{link.id}", dict(**{**dated, "valid_to":"2026-08-01"}, member_id=member.id, relationship="owner")).status_code == 302
    assert link.valid_to.isoformat() == "2026-08-01"
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
    member=register.save_member(data.other.id, data.outsider.id, MEMBER)
    item=register.save_property(data.other.id, data.outsider.id, PROPERTY); db.session.commit()
    assert client.get(BASE + f"/members/{member.id}").status_code == 404
    assert client.safe_post(BASE + f"/properties/{item.id}/edit", PROPERTY).status_code == 404


def test_inactive_intake_rejected(data):
    member=make_member(data, is_active="false")
    with pytest.raises(BadRequest):
        register.intake_links(data.org.id, data.users["manager"].id, member.id)


def test_duplicate_reference_rollback(client, data):
    make_member(data); db.session.commit()
    count=core.CoreOrganizationMember.query.count()
    assert client.safe_post(BASE + "/members/new", MEMBER).status_code == 409
    assert core.CoreOrganizationMember.query.count() == count
    assert uip.UipMemberProfile.query.count() == 1


def test_cross_org_interaction_fk_rejected(data):
    member=make_member(data);item=make_property(data);db.session.flush()
    with pytest.raises(sa.exc.IntegrityError), db.session.begin_nested():
        data.foreign.member_id=member.id
        data.foreign.property_id=item.id
        db.session.flush()


def test_cross_org_preference_and_representation_rejected(data):
    member=make_member(data)
    foreign=register.save_member(data.other.id,data.outsider.id,{**MEMBER,"reference":"FOREIGN"})
    with pytest.raises(NotFound):
        register.set_preference(data.org.id,data.users["manager"].id,foreign.id,dict(channel="Email",preference="allowed"))
    with pytest.raises(NotFound):
        register.save_relationship(data.org.id,data.users["manager"].id,dict(representative_id=foreign.id,valid_from="2026-01-01",is_verified="false"),member_id=member.id)


def test_cross_org_profile_membership_fk_rejected(data):
    membership=core.CoreOrganizationMember.query.filter_by(user_id=data.outsider.id).one()
    with pytest.raises(sa.exc.IntegrityError), db.session.begin_nested():
        db.session.add(uip.UipMemberProfile(organization_id=data.org.id,membership_id=membership.id,reference="X",name="X"))
        db.session.flush()
