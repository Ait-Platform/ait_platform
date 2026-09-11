import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timezone
from flask import Flask
from werkzeug.exceptions import Forbidden

from app.extensions import db, csrf, login_manager
from app.models.core import CoreOrganization, CoreOrganizationMember, CoreRoleAssignment, CoreRole
from app.models.auth import User
from app.models.uip import (UipMemberProfile, UipProperty, UipPropertyMember, 
                            UipMemberRepresentative, UipAuditEvent)
from app.models.uip_governance import UipDelegation
from app.uip.services import register
from app.uip import uip_bp

def test_stage3():
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
    
    with app.app_context():
        # Create tables
        db.metadata.create_all(db.engine, tables=[
            CoreOrganization.__table__, CoreOrganizationMember.__table__,
            CoreRole.__table__, CoreRoleAssignment.__table__,
            User.__table__, UipMemberProfile.__table__, UipProperty.__table__,
            UipPropertyMember.__table__, UipMemberRepresentative.__table__,
            UipDelegation.__table__, UipAuditEvent.__table__
        ])
        
        # Setup Org
        org = CoreOrganization(name="Test Org", slug="test-org")
        db.session.add(org)
        
        # Setup Roles
        role_mgr = CoreRole(slug="manager", name="Manager")
        role_cmt = CoreRole(slug="committee_member", name="Committee Member")
        role_own = CoreRole(slug="owner", name="Owner")
        db.session.add_all([role_mgr, role_cmt, role_own])
        db.session.commit()
        
        # Users
        u_mgr = User(name="Mgr", email="mgr@test", is_active=1)
        u_del = User(name="Delegated", email="del@test", is_active=1)
        u_cmt = User(name="Ord Comm", email="cmt@test", is_active=1)
        u_own = User(name="Owner", email="own@test", is_active=1)
        db.session.add_all([u_mgr, u_del, u_cmt, u_own])
        db.session.commit()
        
        for u in [u_mgr, u_del, u_cmt, u_own]:
            db.session.add(CoreOrganizationMember(organization_id=org.id, user_id=u.id, is_active=True))
            
        db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=u_mgr.id, role_id=role_mgr.id))
        db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=u_del.id, role_id=role_cmt.id))
        db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=u_cmt.id, role_id=role_cmt.id))
        db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=u_own.id, role_id=role_own.id))
        
        # Active Delegation for u_del
        delegation = UipDelegation(
            organization_id=org.id, delegated_user_id=u_del.id, appointed_by_user_id=u_mgr.id,
            delegation_type="RATEPAYER_ADMIN", status="ACTIVE"
        )
        db.session.add(delegation)
        db.session.commit()

        # Test Manager can create ratepayer
        data1 = {"reference": "R1", "name": "John Doe", "member_type": "person", "is_active": "true", "eligibility_status": "unverified"}
        m1 = register.save_member(org.id, u_mgr.id, data1)
        db.session.commit()
        assert m1.reference == "R1"
        assert m1.membership.user_id is None # No linked login user
        assert UipAuditEvent.query.filter_by(action="member.created").count() == 1
        
        # Test Delegate can create property
        data2 = {"reference": "P1", "address": "123 Test St", "classification": "residential", "is_active": "true"}
        p1 = register.save_property(org.id, u_del.id, data2)
        db.session.commit()
        assert p1.address == "123 Test St"
        assert UipAuditEvent.query.filter_by(action="property.created").count() == 1

        # Test Delegate can link ratepayer to property
        rel_data = {"relationship": "owner", "valid_from": "2024-01-01", "is_verified": "false", "member_id": str(m1.id)}
        link1 = register.save_relationship(org.id, u_del.id, rel_data, property_id=p1.id)
        db.session.commit()
        assert link1.relationship == "owner"
        assert UipAuditEvent.query.filter_by(action="ownership.created").count() == 1
        
        # Test Delegate can update ratepayer
        m1_updated = register.save_member(org.id, u_del.id, {"reference": "R1", "name": "John Updated", "member_type": "person", "is_active": "true", "eligibility_status": "eligible"}, member_id=m1.id)
        db.session.commit()
        assert m1_updated.name == "John Updated"
        assert UipAuditEvent.query.filter_by(action="member.updated").count() == 1
        
        # Test Delegate can update property
        p1_updated = register.save_property(org.id, u_del.id, {"reference": "P1", "address": "123 Test Ave", "classification": "residential", "is_active": "true"}, property_id=p1.id)
        db.session.commit()
        assert p1_updated.address == "123 Test Ave"
        assert UipAuditEvent.query.filter_by(action="property.updated").count() == 1
        
        # Test Delegate can end relationship (update valid_to)
        rel_update = {"relationship": "owner", "valid_from": "2024-01-01", "valid_to": "2024-12-31", "is_verified": "false", "member_id": str(m1.id)}
        link1_end = register.save_relationship(org.id, u_del.id, rel_update, property_id=p1.id, link_id=link1.id)
        db.session.commit()
        assert link1_end.valid_to is not None
        assert UipAuditEvent.query.filter_by(action="ownership.updated").count() == 1
        
        # Test Delegate can create representative relationship
        m2 = register.save_member(org.id, u_mgr.id, {"reference": "R2", "name": "Lawyer", "member_type": "business", "is_active": "true", "eligibility_status": "unverified"})
        db.session.commit()
        
        rep_data = {"valid_from": "2024-01-01", "is_verified": "true", "representative_id": str(m2.id)}
        rep_link = register.save_relationship(org.id, u_del.id, rep_data, member_id=m1.id)
        db.session.commit()
        assert rep_link.representative_id == m2.id
        assert UipAuditEvent.query.filter_by(action="representation.created").count() == 1
        
        # Test Delegate can end representative relationship
        rep_update = {"valid_from": "2024-01-01", "valid_to": "2024-12-31", "is_verified": "true", "representative_id": str(m2.id)}
        rep_end = register.save_relationship(org.id, u_del.id, rep_update, member_id=m1.id, link_id=rep_link.id)
        db.session.commit()
        assert rep_end.valid_to is not None
        assert UipAuditEvent.query.filter_by(action="representation.updated").count() == 1
        
        # Test Duplicate protection
        try:
            # Try to create another member with reference R1
            register.save_member(org.id, u_mgr.id, {"reference": "R1", "name": "Another Guy", "member_type": "person", "is_active": "true", "eligibility_status": "unverified"})
            db.session.commit()
            assert False, "Should raise IntegrityError"
        except Exception as e:
            db.session.rollback()
            assert "IntegrityError" in type(e).__name__ or "UniqueConstraint" in str(e)
            
        # Test Ordinary committee member read access
        with app.test_request_context():
            from flask import g
            from flask_login import login_user
            
            # Login as ordinary committee member
            login_user(u_cmt)
            g.organization = org
            
            # Read should succeed
            members_list = register.members(org.id, u_cmt.id).all()
            assert len(members_list) == 2
            
            from app.uip.completion_routes import register_import
            try:
                register_import("test-org")
                assert False, "Import should be forbidden"
            except Forbidden:
                pass
                
        # Revoke delegate and test writes
        delegation.status = "REVOKED"
        db.session.commit()
        
        try:
            register.save_member(org.id, u_del.id, {"reference": "R3", "name": "Alice", "member_type": "person", "is_active": "true", "eligibility_status": "unverified"})
            assert False, "Should be forbidden"
        except Forbidden:
            pass
            
        try:
            register.save_property(org.id, u_del.id, {"reference": "P3", "address": "No", "classification": "residential", "is_active": "true"})
            assert False, "Should be forbidden"
        except Forbidden:
            pass
            
        try:
            rel_new = {"relationship": "owner", "valid_from": "2025-01-01", "is_verified": "false", "member_id": str(m1.id)}
            register.save_relationship(org.id, u_del.id, rel_new, property_id=p1.id)
            assert False, "Should be forbidden"
        except Forbidden:
            pass
                
        print("Stage 3 Additional Tests Passed")

if __name__ == '__main__':
    test_stage3()
