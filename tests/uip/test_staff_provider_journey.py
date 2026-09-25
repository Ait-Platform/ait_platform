"""Existing Staff/provider workspace boundaries, disposable PostgreSQL only."""
import pytest
from bootstrap import db, core, uip
from test_shared_subcommittee_journey import membership_schema
from phase3_helpers import provider, order, act
BASE = "/uip/manor-gardens"


def identities():
    return {m.__tablename__: [tuple(getattr(r,c.name) for c in m.__table__.columns) for r in m.query.order_by(m.id)]
        for m in (core.CoreRoleAssignment, core.CoreOrganizationMember, uip.UipProviderUser)}


@pytest.mark.parametrize("role", ["manager", "receptionist"])
def test_staff_returning_and_entry(client, data, role):
    client.login(role); before=identities()
    for path in ("/dashboard", "/verify/staff"):
        response=client.get(BASE+path)
        assert response.status_code == 302 and response.location.endswith('/operations/reception')
        page=client.get(response.location)
        assert page.status_code == 200 and b"Staff Dashboard" in page.data
    router=client.get(BASE+'/router', follow_redirects=True)
    assert router.status_code == 200 and b"Staff Dashboard" in router.data
    assert client.get(BASE+'/work-orders').status_code == 403
    assert client.get(BASE+'/operations/work-orders').status_code == 200
    assert identities()==before


@pytest.mark.parametrize("path", ["/verify/staff"])
def test_selection_is_pending_not_authority(client, data, path):
    client.login("resident"); before=identities()
    for _ in range(2):
        response=client.get(BASE+path)
        assert response.status_code == 302 and '/my-access' in response.location
    assert core.CoreInteraction.query.filter_by(creator_id=data.users['resident'].id, interaction_type='staff_claim').count()==1
    assert identities()==before
    assert client.get(BASE+'/operations/reception').status_code==403
    assert client.get(BASE+'/work-orders').status_code==403


@pytest.fixture
def dispatched(data):
    p=provider(data); w=order(data,p); act(data,w,'dispatched',dispatch_method='TELEPHONE')
    db.session.commit(); return p,w


def test_provider_returns_to_existing_scoped_workspace(client,data,dispatched):
    client.login('provider'); before=identities()
    response=client.get(BASE+'/dashboard')
    assert response.status_code==302 and response.location.endswith('/provider-dashboard')
    response=client.get(BASE+'/verify/provider')
    assert response.status_code==302 and response.location.endswith('/provider-dashboard')
    page=client.get(BASE+'/router',follow_redirects=True)
    assert page.status_code==200 and b'Provider Dashboard' in page.data
    
    assert client.get(BASE+'/operations/reception').status_code==403
    assert client.get(BASE+'/operations/work-orders').status_code==403
    assert identities()==before


@pytest.mark.parametrize('invalid',['missing','inactive_link','inactive_membership','inactive_provider','missing_role'])
def test_provider_requires_current_authority_and_association(client,data,dispatched,invalid):
    p,w=dispatched
    link=uip.UipProviderUser.query.one()
    if invalid=='missing': db.session.delete(link)
    elif invalid=='inactive_link': link.is_active=False
    elif invalid=='inactive_membership': db.session.get(core.CoreOrganizationMember,link.membership_id).is_active=False
    elif invalid=='inactive_provider': p.is_active=False
    else: core.CoreRoleAssignment.query.filter_by(user_id=data.users['provider'].id).delete()
    db.session.commit(); client.login('provider')
    assert client.get(BASE+'/work-orders').status_code==403
    assert '/provider-dashboard' in client.get(BASE+'/verify/provider').location


def test_other_provider_and_undispatched_work_hidden(client,data,dispatched):
    from app.program_uip.services import providers,work_orders
    from phase3_helpers import key
    p,w=dispatched
    other=providers.save(data.org.id,data.users['manager'].id,dict(name='Other company',availability='AVAILABLE'),['SECURITY'])
    # Separate work order records; no provider association grants for the requesting actor.
    second_issue=core.CoreInteraction(organization_id=data.org.id,creator_id=data.users['resident'].id,title='Other issue',interaction_type='SECURITY',reference='OTHER-WORK',category='SECURITY',status='NEW')
    db.session.add(second_issue);db.session.flush()
    hidden=uip.UipWorkOrder(organization_id=data.org.id,interaction_id=second_issue.id,provider_id=other.id,
        reference='HIDDEN-OTHER',description='Other scope',service_location='Other location',status='DISPATCHED',created_by=data.users['manager'].id)
    db.session.add(hidden);db.session.flush();db.session.commit()
    event=uip.UipAuditEvent(organization_id=data.org.id,actor_user_id=data.users['manager'].id,
        action='work_order.dispatched',entity_type='UipWorkOrder',entity_id=hidden.id)
    db.session.add(event);db.session.flush()
    db.session.add(uip.UipWorkOrderAction(organization_id=data.org.id,work_order_id=hidden.id,
        actor_user_id=data.users['manager'].id,action='dispatched',previous_state='CREATED',new_state='DISPATCHED',
        resulting_version=2,request_key=key(),fingerprint='a'*64,audit_event_id=event.id))
    db.session.commit()
    client.login('provider')
    assert b'HIDDEN-OTHER'  not in client.get(BASE+'/work-orders').data
    assert client.get(BASE+f'/work-orders/{hidden.id}').status_code==404
    # A fresh own-provider order without dispatch evidence is also unavailable.
    third_issue=core.CoreInteraction(organization_id=data.org.id,creator_id=data.users['resident'].id,
        title='Undispatched issue',reference='UNDISPATCHED-ISSUE',interaction_type='SECURITY',status='NEW')
    db.session.add(third_issue);db.session.flush()
    own_draft=uip.UipWorkOrder(organization_id=data.org.id,interaction_id=third_issue.id,provider_id=p.id,
        reference='OWN-UNDISPATCHED',description='Draft scope',service_location='Entrance',status='CREATED',created_by=data.users['manager'].id)
    db.session.add(own_draft);db.session.commit()
    assert own_draft.reference.encode() not in client.get(BASE+'/work-orders').data
    assert client.get(BASE+f'/work-orders/{own_draft.id}').status_code==404


def test_cross_org_staff_denied(client,data):
    client.login('outsider')
    assert client.get(BASE+'/operations/reception').status_code==403
    assert client.get(BASE+'/operations/work-orders').status_code==403
    assert client.get(BASE+'/work-orders').status_code==403


@pytest.fixture
def secretary(data):
    from app.models.uip_governance import UipCommitteeTerm, UipCommitteeMember
    term=UipCommitteeTerm(organization_id=data.org.id,term_name="Current elected term")
    db.session.add(term);db.session.flush()
    user=data.users['receptionist']
    db.session.add(UipCommitteeMember(organization_id=data.org.id,term_id=term.id,name=user.name,
        email=user.email,user_id=user.id,position='Secretary',status='CURRENT'))
    db.session.commit()
    return user


def request_access(client,data,kind):
    client.login('resident')
    before=identities()
    result=client.get(BASE+'/verify/'+kind)
    assert result.status_code==302 and '/my-access' in result.location
    assert identities()==before
    return core.CoreInteraction.query.filter_by(organization_id=data.org.id,creator_id=data.users['resident'].id,
        interaction_type='staff_claim',status='OPEN').one()


def approval_values(claim,role):
    return {'claim_ids[]':str(claim.id),'resolution_target':'operational',f'operational_role_{claim.id}':role}


@pytest.mark.parametrize('kind,role',[('staff','receptionist')])
def test_secretary_verification_and_returning_journey(client,data,secretary,kind,role):
    claim=request_access(client,data,kind)
    client.login('receptionist')
    board=client.get(BASE+'/secretary-workspace')
    assert board.status_code==200 and b'bg-red-50' in board.data and b'1 claims pending verification' in board.data
    inbox=client.get(BASE+'/secretary-intake')
    assert inbox.status_code==200 and claim.title.encode() in inbox.data
    form=client.safe_post(BASE+'/draft-access-resolution',{'claim_ids[]':str(claim.id)})
    assert form.status_code==200 and b'Verify and admit' in form.data
    values=approval_values(claim,role)
    assert client.safe_post(BASE+'/finalize-access-resolution',values).status_code==302
    assert client.safe_post(BASE+'/finalize-access-resolution',values).status_code==302
    assert claim.status=='VERIFIED' and claim.closed_by==secretary.id
    grant=core.CoreRoleAssignment.query.join(core.CoreRole).filter(
        core.CoreRoleAssignment.user_id==data.users['resident'].id,core.CoreRoleAssignment.organization_id==data.org.id,
        core.CoreRole.slug==role).all()
    assert len(grant)==1
    assert uip.UipAuditEvent.query.filter_by(action='OPERATIONAL_ACCESS_VERIFIED',entity_id=claim.id,actor_user_id=secretary.id).count()==1
    assert uip.UipProviderUser.query.count()==0
    assert uip.UipResolution.query.count()==0
    client.login('resident')
    page=client.get(BASE+'/router',follow_redirects=True)
    if kind=='staff':
        assert page.status_code==200 and b'Staff Dashboard' in page.data
    else:
        assert client.get(BASE+'/work-orders').status_code==403
        # An authorized provider still needs the existing separate manager-recorded association.
        from app.program_uip.services import providers
        company=providers.save(data.org.id,data.users['manager'].id,dict(name='Approved company',availability='AVAILABLE'),['SECURITY'])
        membership=core.CoreOrganizationMember.query.filter_by(organization_id=data.org.id,user_id=data.users['resident'].id).one()
        providers.associate(data.org.id,data.users['manager'].id,company.id,membership.id,company.version)
        db.session.commit()
        page=client.get(BASE+'/router',follow_redirects=True)
        assert page.status_code==200 and b'Provider Dashboard' in page.data


@pytest.mark.parametrize('kind',['staff'])
def test_claimant_cannot_self_admit(client,data,secretary,kind):
    claim=request_access(client,data,kind)
    before=identities()
    assert client.safe_post(BASE+'/finalize-access-resolution',approval_values(claim,'receptionist')).status_code==403
    assert claim.status=='OPEN' and identities()==before


@pytest.mark.parametrize('forged_role',['owner','committee_member','municipal_officer','subcommittee_member','provider'])
def test_operational_admission_cannot_assign_governance_or_wrong_journey(client,data,secretary,forged_role):
    claim=request_access(client,data,'staff')
    client.login('receptionist');before=identities()
    assert client.safe_post(BASE+'/finalize-access-resolution',approval_values(claim,forged_role)).status_code==403
    assert claim.status=='OPEN' and identities()==before


def test_staff_claim_cannot_use_old_owner_approval_fallback(client,data,secretary):
    claim=request_access(client,data,'staff');client.login('receptionist');before=identities()
    assert client.safe_post(BASE+'/finalize-access-resolution',{'claim_ids[]':str(claim.id),'resolution_target':'instant'}).status_code==400
    assert identities()==before and claim.status=='OPEN'


def test_owner_is_not_secretary_gatekeeper(client,data,secretary):
    claim=request_access(client,data,'staff');client.login('owner');before=identities()
    assert client.safe_post(BASE+'/finalize-access-resolution',approval_values(claim,'receptionist')).status_code==403
    assert identities()==before


@pytest.mark.parametrize('kind',['staff','legacy'])
def test_secretary_cannot_grant_legacy_manager(client,data,secretary,kind):
    claim=request_access(client,data,'staff')
    if kind=='legacy':
        claim.category=None
        claim.description='Legacy combined Staff / Provider request'
        db.session.commit()
    client.login('receptionist')
    form=client.safe_post(BASE+'/draft-access-resolution',{'claim_ids[]':str(claim.id)})
    assert form.status_code==200 and b'value="manager"' not in form.data
    before=identities()
    assert client.safe_post(BASE+'/finalize-access-resolution',approval_values(claim,'manager')).status_code==403
    assert identities()==before and claim.status=='OPEN'


# Skipped provider pending tests
@pytest.mark.skip(reason="Provider no longer creates pending claims")
@pytest.mark.parametrize("membership_state", ["missing", "inactive"])
def test_provider_pending_without_active_membership(client, data, secretary, membership_state):
    user = data.users["resident"]
    member = core.CoreOrganizationMember.query.filter_by(organization_id=data.org.id, user_id=user.id).one()
    if membership_state == "missing":
        db.session.delete(member)
    else:
        member.is_active = False
    db.session.commit()
    claim = request_access(client, data, "staff")
    response = client.get(BASE + "/verify/provider")
    assert response.location.endswith("/my-access?claim=provider")
    page = client.get(response.location)
    assert page.status_code == 200 and b"Service Provider Access Request" in page.data
    assert b"Staff / Service Provider" not in page.data
    client.login("receptionist")
    assert claim.title.encode() in client.get(BASE + "/secretary-intake").data
    assert client.safe_post(BASE + "/finalize-access-resolution", approval_values(claim, "provider")).status_code == 302
    assert core.CoreOrganizationMember.query.filter_by(organization_id=data.org.id, user_id=user.id).one().is_active
    assert core.CoreRoleAssignment.query.join(core.CoreRole).filter(core.CoreRoleAssignment.user_id == user.id,
        core.CoreRoleAssignment.organization_id == data.org.id, core.CoreRole.slug == "provider").count() == 1
    assert uip.UipProviderUser.query.count() == 0


def test_staff_legacy_reference_links_and_search(client, data):
    import re
    issue = core.CoreInteraction(organization_id=data.org.id, creator_id=data.users["resident"].id,
        interaction_type="staff_claim", title="Legacy pending request", reference=None, status="OPEN")
    db.session.add(issue); db.session.commit()
    client.login("receptionist")
    for suffix in ("", "?q=Legacy"):
        response = client.get(BASE + "/operations/reception" + suffix)
        assert response.status_code == 200
        assert (BASE + f"/operations/reception/{issue.id}").encode() in response.data
        links = re.findall(r'href="([^"]+)"', response.data.decode())
        for link in links:
            if link.startswith(BASE + "/operations/reception/"):
                assert client.get(link).status_code == 200
