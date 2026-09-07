import pytest
from werkzeug.exceptions import Forbidden, Conflict, NotFound
from bootstrap import db, core, uip
from app.uip.services import providers, work_orders
from phase3_helpers import provider, order, act


def test_register_capabilities_availability_and_no_role_grants(data):
    count=core.CoreRoleAssignment.query.count();p=provider(data)
    assert providers.eligible(data.org.id,p,"SECURITY")
    assert not providers.eligible(data.org.id,p,"CLEANING")
    assert core.CoreRoleAssignment.query.count()==count
    providers.save(data.org.id,data.users["manager"].id,dict(name="Changed",availability="UNAVAILABLE"),["CLEANING"],p.id,p.version)
    assert not providers.eligible(data.org.id,p,"CLEANING")
    assert uip.UipProviderCapability.query.one().category=="CLEANING"


@pytest.mark.parametrize("actor",["receptionist","provider","committee_member","owner","resident"])
def test_only_manager_controls_register(data,actor):
    with pytest.raises(Forbidden):providers.save(data.org.id,data.users[actor].id,dict(name="No"),[])


def test_role_alone_not_association_and_revoke_immediate(data):
    p=provider(data);row=order(data,p);act(data,row,"dispatched",dispatch_method="EMAIL")
    link=uip.UipProviderUser.query.one()
    providers.revoke(data.org.id,data.users["manager"].id,p.id,link.id,p.version)
    assert not providers.linked(data.org.id,p.id,data.users["provider"].id)
    with pytest.raises(NotFound):act(data,row,"accepted")
    assert link.revoked_at and link.revoked_by and not link.is_active


def test_deactivation_blocked_by_outstanding_work(data):
    p=provider(data);row=order(data,p)
    with pytest.raises(Conflict):providers.deactivate(data.org.id,data.users["manager"].id,p.id,p.version)
    act(data,row,"cancelled",note="Not needed",reason_code="NOT_REQUIRED")
    providers.deactivate(data.org.id,data.users["manager"].id,p.id,p.version)
    assert not p.is_active


def test_dispatch_rechecks_eligibility(data):
    p=provider(data);row=order(data,p)
    p.availability="UNAVAILABLE";db.session.flush()
    with pytest.raises(Conflict):act(data,row,"dispatched",dispatch_method="EMAIL")
    p.availability="AVAILABLE";act(data,row,"dispatched",dispatch_method="EMAIL")
    p.availability="UNAVAILABLE";act(data,row,"accepted");act(data,row,"started");act(data,row,"completed")
    assert row.status=="COMPLETED"


def test_cross_org_links_rejected_and_inactive_user_cannot_act(data):
    p=provider(data)
    foreign=core.CoreOrganizationMember.query.filter_by(organization_id=data.other.id).first()
    with pytest.raises(NotFound):providers.associate(data.org.id,data.users["manager"].id,p.id,foreign.id,p.version)
    data.users["provider"].is_active=0;db.session.flush()
    assert not providers.eligible(data.org.id,p,"SECURITY")


def test_provider_versions_reject_stale_edits(data):
    p=provider(data)
    with pytest.raises(Conflict):providers.save(data.org.id,data.users["manager"].id,dict(name="Bad"),[],p.id,0)


def test_association_does_not_grant_manager_and_manager_cannot_self_verify(data):
    from phase3_helpers import completed
    row=completed(data)
    membership=core.CoreOrganizationMember.query.filter_by(organization_id=data.org.id,user_id=data.users["manager"].id).one()
    # Existing dual-role association is a conflict of interest even if its provider role is later revoked.
    db.session.add(uip.UipProviderUser(organization_id=data.org.id,provider_id=row.provider_id,membership_id=membership.id,linked_by=data.users["manager"].id))
    db.session.flush()
    with pytest.raises(Forbidden):act(data,row,"verified")


def test_database_tenant_foreign_keys_prevent_cross_organisation_records(data):
    import sqlalchemy as sa
    p=provider(data)
    foreign=core.CoreOrganizationMember.query.filter_by(organization_id=data.other.id).first()
    with pytest.raises(sa.exc.IntegrityError),db.session.begin_nested():
        db.session.add(uip.UipProviderUser(organization_id=data.org.id,provider_id=p.id,membership_id=foreign.id,linked_by=data.users["manager"].id))
        db.session.flush()
    with pytest.raises(sa.exc.IntegrityError),db.session.begin_nested():
        db.session.add(uip.UipProviderCapability(organization_id=data.other.id,provider_id=p.id,category="SECURITY"))
        db.session.flush()
    with pytest.raises(sa.exc.IntegrityError),db.session.begin_nested():
        db.session.add(uip.UipWorkOrder(organization_id=data.org.id,provider_id=p.id,interaction_id=data.foreign.id,
            created_by=data.users["manager"].id,status="CREATED",service_location="Invalid",version=1))
        db.session.flush()
