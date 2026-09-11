import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timezone
from flask import Flask
from app.extensions import db
from app.models.core import CoreOrganization, CoreOrganizationMember, CoreRoleAssignment, CoreRole
from app.models.auth import User
from app.models.uip import UipCommitteeMeeting, UipResolution

def test_founding_direct():
    app = Flask(__name__)
    app.config["TESTING"] = True
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["WTF_CSRF_ENABLED"] = False
    
    db.init_app(app)
    
    with app.app_context():
        db.metadata.create_all(db.engine, tables=[
            CoreOrganization.__table__, CoreOrganizationMember.__table__,
            CoreRole.__table__, CoreRoleAssignment.__table__,
            User.__table__, UipCommitteeMeeting.__table__,
            UipResolution.__table__
        ])
        
        org = CoreOrganization(name="Test Org", slug="test-org")
        db.session.add(org)
        user = User(name="Manager", email="manager@test.com", is_active=1)
        db.session.add(user)
        db.session.flush()
        
        member = CoreOrganizationMember(organization_id=org.id, user_id=user.id, is_active=True)
        db.session.add(member)
        role = CoreRole(name="manager", slug="manager")
        db.session.add(role)
        db.session.flush()
        
        assignment = CoreRoleAssignment(organization_id=org.id, user_id=user.id, role_id=role.id)
        db.session.add(assignment)
        db.session.commit()
        
        # Test models directly
        meeting = UipCommitteeMeeting(
            organization_id=org.id,
            title="Founding AGM",
            meeting_type="FOUNDING",
            scheduled_at=datetime(2026, 9, 1, 18, 0, tzinfo=timezone.utc),
            location="Town Hall",
            status="CONCLUDED"
        )
        db.session.add(meeting)
        db.session.flush()

        new_user = User(name="Alice", email="alice@test.com", is_active=0)
        db.session.add(new_user)
        db.session.flush()

        membership = CoreOrganizationMember(
            organization_id=org.id, 
            user_id=new_user.id, 
            is_active=False
        )
        db.session.add(membership)
        db.session.flush()

        election_resolution = UipResolution(
            organization_id=org.id,
            meeting_id=meeting.id,
            title="Election of Committee Members",
            description="The following individuals were elected to the committee at the founding meeting.",
            recorded_by=user.id,
            result_basis={"elected_committee_user_ids": [new_user.id]}
        )
        db.session.add(election_resolution)

        manager_resolution = UipResolution(
            organization_id=org.id,
            meeting_id=meeting.id,
            title="Manager Designation",
            description="Alice is Manager",
            recorded_by=user.id,
            responsible_user_id=new_user.id
        )
        db.session.add(manager_resolution)
        db.session.commit()
        
        assert UipCommitteeMeeting.query.count() == 1
        assert User.query.filter_by(is_active=0).count() == 1
        assert CoreOrganizationMember.query.filter_by(is_active=False).count() == 1
        assert UipResolution.query.count() == 2
        
        print("Model constraints for founding are valid.")

if __name__ == "__main__":
    test_founding_direct()
