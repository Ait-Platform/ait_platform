import os
import sys
import datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.models.core import CoreOrganization, CoreOrganizationMember, CoreRole, CoreRoleAssignment
from app.models.auth import User
from app.models.uip import UipMemberProfile, UipProperty, UipPropertyMember, UipRegisterImport, UipRegisterImportException, UipDocument, UipAuditEvent
from app.uip.services import register
from werkzeug.exceptions import Forbidden, Conflict
from app.extensions import db
from flask import Flask

def run_tests():
    app = Flask(__name__)
    app.config["TESTING"] = True
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    db.init_app(app)
    
    with app.app_context():
        # Setup tables manually instead of flask db migrate to avoid conflicts
        db.metadata.create_all(db.engine, tables=[
            CoreOrganization.__table__, User.__table__, CoreOrganizationMember.__table__,
            UipDocument.__table__, UipRegisterImport.__table__, UipRegisterImportException.__table__,
            UipMemberProfile.__table__, UipProperty.__table__, UipPropertyMember.__table__,
            UipAuditEvent.__table__, CoreRole.__table__, CoreRoleAssignment.__table__
        ])

        db.session.rollback()
        org = CoreOrganization(name="Test UIP", slug="test-uip")
        admin = User(email="admin@test.com", password_hash="hash", is_active=True)
        user = User(email="user@test.com", password_hash="hash", is_active=True)
        db.session.add_all([org, admin, user])
        db.session.flush()
        
        member = CoreOrganizationMember(organization_id=org.id, user_id=admin.id, is_active=True)
        db.session.add(member)
        db.session.flush()
        
        # Grant admin manager role via CoreRole
        role = CoreRole(name="Manager", slug="manager")
        db.session.add(role)
        db.session.flush()
        assign = CoreRoleAssignment(organization_id=org.id, user_id=admin.id, role_id=role.id)
        db.session.add(assign)
        db.session.flush()
        
        # Grant admin RATEPAYER_ADMIN via UipDelegation just in case
        from app.models.uip_governance import UipDelegation
        db.metadata.create_all(db.engine, tables=[UipDelegation.__table__])
        delg = UipDelegation(organization_id=org.id, delegated_user_id=admin.id, appointed_by_user_id=admin.id, delegation_type="RATEPAYER_ADMIN", status="ACTIVE", effective_date=datetime.date.today())
        db.session.add(delg)
        db.session.commit()

        print("ROLES", CoreRole.query.all())
        print("ASSIGNMENTS", CoreRoleAssignment.query.all())
        print("DELG", UipDelegation.query.all())

        # 10. Manager/RATEPAYER_ADMIN cannot manually create an authoritative ratepayer/property
        try:
            register.save_member(org.id, admin.id, {"reference": "M1", "name": "Test M1"}, is_import=False)
            assert False, "Should block manual creation of ratepayer"
        except Forbidden:
            pass
            
        try:
            register.save_property(org.id, admin.id, {"reference": "P1", "address": "123 Main St", "classification": "residential"}, is_import=False)
            assert False, "Should block manual creation of property"
        except Forbidden:
            pass

        # 12. RATEPAYER_ADMIN can run the authorised municipal import
        metadata = {
            "source_identifier": "City of Test",
            "date_received": datetime.date.today(),
            "effective_date": datetime.date.today()
        }
        
        # 1. New municipal ratepayer/property created from authorised batch
        rows_m = [{"reference": "M-NEW", "name": "New Ratepayer", "member_type": "person", "is_active": "true", "eligibility_status": "unverified"}]
        batch_m, sum_m = register.process_import_batch(org.id, admin.id, "members", rows_m, metadata)
        if sum_m["created"] != 1:
            excs = UipRegisterImportException.query.filter_by(import_id=batch_m.id).all()
            for e in excs:
                print(e.reason, getattr(e, "description", None))
        assert sum_m["created"] == 1
        
        m_new = UipMemberProfile.query.filter_by(reference="M-NEW").first()
        assert m_new.record_source == "MUNICIPAL"
        assert m_new.last_import_id == batch_m.id
        
        # 4. Exact legacy MANUAL reference -> converted safely to MUNICIPAL
        m_leg = UipMemberProfile(organization_id=org.id, membership_id=999, reference="M-LEG", name="Legacy", email="keep@this.com", phone="123", record_source="MANUAL", member_type="person", is_active=True, eligibility_status="unverified")
        db.session.add(m_leg)
        db.session.flush()
        
        rows_leg = [{"reference": "M-LEG", "name": "Converted", "member_type": "business", "is_active": "true", "eligibility_status": "unverified"}]
        batch_leg, sum_leg = register.process_import_batch(org.id, admin.id, "members", rows_leg, metadata)
        assert sum_leg["updated"] == 1
        m_leg_updated = UipMemberProfile.query.get(m_leg.id)
        assert m_leg_updated.record_source == "MUNICIPAL"
        assert m_leg_updated.name == "Converted"
        if m_leg_updated.email != "keep@this.com":
            print(f"Condition 20 FAILED: Email wiped out, got {m_leg_updated.email}")
            assert m_leg_updated.email == "keep@this.com"

        # 6. Conflicting row -> persistent import exception (e.g. duplicate reference already caught in process, or missing reference)
        rows_err = [{"reference": "", "name": "No Ref"}]
        batch_err, sum_err = register.process_import_batch(org.id, admin.id, "members", rows_err, metadata)
        assert sum_err["exceptions"] == 1
        assert UipRegisterImportException.query.filter_by(import_id=batch_err.id).count() == 1
        
        # 3. Existing municipal record changed -> sealed fields updated and audit event contains import_id
        rows_upd = [{"reference": "M-NEW", "name": "Updated Ratepayer", "member_type": "person", "is_active": "true", "eligibility_status": "unverified"}]
        batch_upd, sum_upd = register.process_import_batch(org.id, admin.id, "members", rows_upd, metadata)
        assert sum_upd["updated"] == 1
        m_new_updated = UipMemberProfile.query.get(m_new.id)
        assert m_new_updated.name == "Updated Ratepayer"
        audit = UipAuditEvent.query.filter_by(entity_id=m_new.id, action="member.updated").order_by(UipAuditEvent.id.desc()).first()
        assert audit.metadata_json.get("import_id") == batch_upd.id

        # 8. Ownership change closes prior relationship at batch effective date and creates new current relationship
        # First create property and relationships via import
        rows_p = [{"reference": "P-1", "address": "Property 1", "classification": "residential", "is_active": "true"}]
        register.process_import_batch(org.id, admin.id, "properties", rows_p, metadata)
        
        rows_rel1 = [{"member_reference": "M-LEG", "property_reference": "P-1", "relationship": "owner", "is_verified": "true"}]
        batch_rel1, _ = register.process_import_batch(org.id, admin.id, "relationships", rows_rel1, metadata)
        
        # Now change owner
        metadata["effective_date"] = datetime.date.today() + datetime.timedelta(days=1)
        rows_rel2 = [{"member_reference": "M-NEW", "property_reference": "P-1", "relationship": "owner", "is_verified": "true"}]
        batch_rel2, _ = register.process_import_batch(org.id, admin.id, "relationships", rows_rel2, metadata)
        
        prop = UipProperty.query.filter_by(reference="P-1").first()
        rels = UipPropertyMember.query.filter_by(property_id=prop.id).all()
        print("RELS:", len(rels), rels)
        excs_rel1 = UipRegisterImportException.query.filter_by(import_id=batch_rel1.id).all()
        excs_rel2 = UipRegisterImportException.query.filter_by(import_id=batch_rel2.id).all()
        print("EXCS REL1:", [(e.reason, e.incoming_data) for e in excs_rel1])
        print("EXCS REL2:", [(e.reason, e.incoming_data) for e in excs_rel2])
        assert len(rels) == 2
        # One is closed
        closed = [r for r in rels if r.valid_to is not None][0]
        active = [r for r in rels if r.valid_to is None][0]
        assert closed.member_id == m_leg.id
        assert closed.valid_to == metadata["effective_date"]
        assert active.member_id == m_new.id
        
        print("Stage 3.5 Integration Tests Passed")
        
if __name__ == '__main__':
    run_tests()
