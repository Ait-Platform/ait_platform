import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timezone
from flask import Flask
from itsdangerous import URLSafeTimedSerializer

from app.extensions import db, csrf, login_manager
from app.models.core import CoreOrganization, CoreOrganizationMember, CoreRoleAssignment, CoreRole
from app.models.auth import User
from app.models.uip import UipCommitteeMeeting, UipResolution
from app.models.uip_governance import UipDelegation
from app.uip.services.governance import has_delegation
from app.uip import uip_bp
from app.auth.routes import auth_bp

def test_stage2():
    app = Flask(__name__)
    app.config["TESTING"] = True
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["WTF_CSRF_ENABLED"] = False
    app.config["SECRET_KEY"] = "test_secret_key"
    
    db.init_app(app)
    csrf.init_app(app)
    login_manager.init_app(app)
    
    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))
        
    app.register_blueprint(uip_bp, url_prefix='/uip')
    app.register_blueprint(auth_bp, url_prefix='/auth')
    
    with app.app_context():
        # Create all tables including UipDelegation
        db.metadata.create_all(db.engine, tables=[
            CoreOrganization.__table__, CoreOrganizationMember.__table__,
            CoreRole.__table__, CoreRoleAssignment.__table__,
            User.__table__, UipCommitteeMeeting.__table__,
            UipResolution.__table__, UipDelegation.__table__
        ])
        
        # Seed Roles
        db.session.add(CoreRole(slug="manager", name="Manager"))
        db.session.add(CoreRole(slug="committee_member", name="Committee Member"))
        
        org = CoreOrganization(name="Test Org", slug="test-org")
        db.session.add(org)
        db.session.flush()

        # Seed Stage 1 Founding
        meeting = UipCommitteeMeeting(
            organization_id=org.id, title="Founding AGM",
            meeting_type="FOUNDING", scheduled_at=datetime.now(timezone.utc),
            location="Town Hall", status="CONCLUDED"
        )
        db.session.add(meeting)
        db.session.flush()

        # Inactive elected members
        manager_user = User(name="Alice Manager", email="alice@test.com", is_active=0)
        committee_user = User(name="Bob Member", email="bob@test.com", is_active=0)
        random_user = User(name="Charlie Random", email="charlie@test.com", is_active=0)
        
        for u in [manager_user, committee_user, random_user]:
            db.session.add(u)
        db.session.flush()
        
        for u in [manager_user, committee_user]:
            db.session.add(CoreOrganizationMember(organization_id=org.id, user_id=u.id, is_active=False))
            
        elec_res = UipResolution(
            organization_id=org.id, meeting_id=meeting.id,
            title="Election of Committee Members",
            result_basis={"elected_committee_user_ids": [manager_user.id, committee_user.id]}
        )
        man_res = UipResolution(
            organization_id=org.id, meeting_id=meeting.id,
            title="Manager Designation",
            responsible_user_id=manager_user.id
        )
        db.session.add(elec_res)
        db.session.add(man_res)
        db.session.commit()

        # --- Test 1: Activation ---
        signer = URLSafeTimedSerializer(app.secret_key, salt="uip-committee-activation")
        
        with app.test_client() as client:
            # Activate Manager
            token_alice = signer.dumps(manager_user.id)
            res = client.post(f"/uip/test-org/activate-committee/{token_alice}", data={"password": "password123", "csrf_token": ""})
            assert res.status_code == 302
            
            # Activate Committee Member
            token_bob = signer.dumps(committee_user.id)
            res = client.post(f"/uip/test-org/activate-committee/{token_bob}", data={"password": "password123", "csrf_token": ""})
            assert res.status_code == 302
            
            # Try to activate Random User (not in election) - MUST FAIL 403
            token_charlie = signer.dumps(random_user.id)
            res = client.post(f"/uip/test-org/activate-committee/{token_charlie}", data={"password": "password123", "csrf_token": ""})
            assert res.status_code == 403
            
        # Verify Roles and Activation Status
        assert manager_user.is_active == 1
        roles_alice = [r.role.slug for r in CoreRoleAssignment.query.filter_by(user_id=manager_user.id).all()]
        assert "manager" in roles_alice
        assert "committee_member" in roles_alice
        
        assert committee_user.is_active == 1
        roles_bob = [r.role.slug for r in CoreRoleAssignment.query.filter_by(user_id=committee_user.id).all()]
        assert "manager" not in roles_bob
        assert "committee_member" in roles_bob
        
        assert random_user.is_active == 0
        roles_charlie = [r.role.slug for r in CoreRoleAssignment.query.filter_by(user_id=random_user.id).all()]
        assert len(roles_charlie) == 0
        
        print("Activation test passed.")

        # --- Test 2: Delegation ---
        with app.test_request_context():
            from flask import g
            from flask_login import login_user
            from app.uip.committee_routes import delegate_ratepayer_admin
            from werkzeug.test import EnvironBuilder
            from flask import request
            
            # Simulate Manager Login
            login_user(manager_user)
            g.organization = org
            g.uip_roles = set(roles_alice)
            
            # Delegate to Bob (valid active committee member)
            builder = EnvironBuilder(method='POST')
            app.request_class = type('Request', (app.request_class,), {})
            request.environ = builder.get_environ()
            request.form = {"delegated_user_id": str(committee_user.id)}
            
            try:
                delegate_ratepayer_admin("test-org")
            except Exception:
                pass # redirect
            db.session.commit()
            
            assert has_delegation(org.id, committee_user.id, "RATEPAYER_ADMIN") is True
            assert has_delegation(org.id, manager_user.id, "RATEPAYER_ADMIN") is False
            
            # Attempt to delegate to Charlie (unelected, inactive) - MUST REJECT
            request.form = {"delegated_user_id": str(random_user.id)}
            try:
                delegate_ratepayer_admin("test-org")
            except Exception as e:
                assert e.code == 400
            db.session.commit()
            
            # Re-delegate to Alice (revoke Bob)
            request.form = {"delegated_user_id": str(manager_user.id)}
            try:
                delegate_ratepayer_admin("test-org")
            except:
                pass
            db.session.commit()
            
            assert has_delegation(org.id, committee_user.id, "RATEPAYER_ADMIN") is False
            assert has_delegation(org.id, manager_user.id, "RATEPAYER_ADMIN") is True
            
            # Explicit Revoke Without Replacement
            request.form = {"delegated_user_id": ""}
            try:
                delegate_ratepayer_admin("test-org")
            except:
                pass
            db.session.commit()
            
            assert has_delegation(org.id, manager_user.id, "RATEPAYER_ADMIN") is False
            assert UipDelegation.query.filter_by(status="ACTIVE", delegation_type="RATEPAYER_ADMIN").count() == 0
            
            print("Delegation test passed.")

if __name__ == "__main__":
    test_stage2()
