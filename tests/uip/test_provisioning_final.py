import pytest
from app.models.auth import User
from app.models.core import CoreOrganization, CoreRoleAssignment, CoreRole
from app.models.uip_governance import UipCommitteeTerm, UipCommitteeMember, UipDelegation
from app.models.uip import UipCommitteeMeeting, UipResolution
from bs4 import BeautifulSoup

def test_anonymous_provisioning_flow(client, app):
    with app.app_context():
        # Setup an org
        org = CoreOrganization(name="Test UIP", slug="test-uip")
        db = app.extensions['sqlalchemy']
        db.session.add(org)
        db.session.commit()
        org_id = org.id
        
        # Ensure it has no founding meeting
        assert UipCommitteeMeeting.query.filter_by(organization_id=org_id, meeting_type="FOUNDING").count() == 0

    # 1. anonymous GET to initial provisioning page succeeds before provisioning
    res = client.get('/uip/test-uip/provisioning')
    assert res.status_code == 200
    assert b"Founding Meeting" in res.data
    
    # 2. anonymous POST can create the first committee term/list
    post_data = {
        "venue": "Town Hall",
        "meeting_date": "2026-09-12",
        "meeting_time": "10:00",
        "member_name[]": ["Alice Chair", "Bob Member"],
        "member_email[]": ["alice@example.com", "bob@example.com"],
        "member_position[]": ["Chairperson", "Committee Member"],
        "manager_index": "0",
        "resolution_text": "Alice is manager"
    }
    res = client.post('/uip/test-uip/provisioning', data=post_data, follow_redirects=True)
    assert res.status_code == 200
    assert b"Founding committee successfully provisioned." in res.data
    
    with app.app_context():
        # 3. first submission creates committee records correctly
        meeting = UipCommitteeMeeting.query.filter_by(organization_id=org_id, meeting_type="FOUNDING").first()
        assert meeting is not None
        assert meeting.location == "Town Hall"
        
        term = UipCommitteeTerm.query.filter_by(organization_id=org_id).first()
        assert term is not None
        
        members = UipCommitteeMember.query.filter_by(term_id=term.id).all()
        assert len(members) == 2
        assert members[0].name == "Alice Chair"
        assert members[0].status == "CURRENT"
        assert members[0].created_by is None  # Anonymous!
        
        # 4. submitter gains no role/delegation/authority
        # Since it's anonymous, there is no submitter user to check, but let's check Alice (who was created as a pending user)
        alice = User.query.filter_by(email="alice@example.com").first()
        assert alice is not None
        assert alice.is_active == 0  # Still pending application login
        
        # Alice is the manager in resolution, but does she have a CoreRoleAssignment?
        # NO! Manager authority remains designated via resolution, not role assignment automatically by the form.
        manager_role = CoreRole.query.filter_by(slug="uip_manager").first()
        if manager_role:
            assert CoreRoleAssignment.query.filter_by(user_id=alice.id, role_id=manager_role.id).count() == 0
        
        assert UipDelegation.query.filter_by(delegated_user_id=alice.id).count() == 0

    # 5. second anonymous provisioning attempt cannot create/replace the initial committee
    res2 = client.post('/uip/test-uip/provisioning', data=post_data, follow_redirects=True)
    assert res2.status_code == 200
    assert b"This organisation has already been provisioned." in res2.data
    
    with app.app_context():
        # Still only one founding meeting
        assert UipCommitteeMeeting.query.filter_by(organization_id=org_id, meeting_type="FOUNDING").count() == 1

def test_committee_member_routing(client, app):
    with app.app_context():
        db = app.extensions['sqlalchemy']
        org = CoreOrganization(name="Auth UIP", slug="auth-uip")
        db.session.add(org)
        
        # Create normal registered user
        user = User(name="Registered User", email="registered@example.com", is_active=1)
        user.set_password("password")
        db.session.add(user)
        
        # Unmatched user
        unmatched = User(name="Unmatched", email="unmatched@example.com", is_active=1)
        unmatched.set_password("password")
        db.session.add(unmatched)
        db.session.commit()
        
        term = UipCommitteeTerm(organization_id=org.id, term_name="Test Term")
        db.session.add(term)
        db.session.commit()
        
        mem = UipCommitteeMember(term_id=term.id, organization_id=org.id, name="Reg", email="registered@example.com", position="Chairperson", status="CURRENT")
        db.session.add(mem)
        db.session.commit()
        
    # Test matched user
    client.post('/login', data={'email': 'registered@example.com', 'password': 'password'})
    res = client.get('/uip/auth-uip/verify-committee', follow_redirects=True)
    assert res.status_code == 200
    # Should reach dashboard (Committee Member)
    assert b"Committee Dashboard" in res.data or b"committee-dashboard" in res.request.url
    client.get('/logout')
    
    # Test unmatched user
    client.post('/login', data={'email': 'unmatched@example.com', 'password': 'password'})
    res = client.get('/uip/auth-uip/verify-committee', follow_redirects=True)
    assert res.status_code == 200
    # Should see verification pending
    assert b"You do not have a current committee record" in res.data or b"Your account is not matched to a current committee record" in res.data

def test_existing_committee_manage_flow(client, app):
    # Ensure /uip/<slug>/committee/manage remains protected
    res = client.get('/uip/auth-uip/committee/manage')
    assert res.status_code == 302
    assert b"/login" in res.data
