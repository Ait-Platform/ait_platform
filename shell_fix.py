from wsgi import app
from app.extensions import db
from app.models.uip_governance import UipCommitteeMember
from app.models.core import CoreInteraction, CoreRoleAssignment, CoreRole

with app.app_context():
    members = UipCommitteeMember.query.all()
    for m in members:
        if m.position in ["Committee", "Unassigned", "Committee Member"]:
            print(f"Fixing {m.name}...")
            claim = CoreInteraction.query.filter_by(creator_id=m.user_id, interaction_type="committee_claim").first()
            if claim and ":" in claim.title:
                m.position = claim.title.split(": ")[-1]
                print(f" -> Set to {m.position}")
            else:
                m.position = "Chairperson"
                print(" -> Set to Chairperson (fallback)")
                
    # Also fix CoreRoleAssignments for Chairperson and Treasurer just in case!
    role = CoreRole.query.filter_by(slug="committee_member").first()
    if role:
        for m in members:
            existing = CoreRoleAssignment.query.filter_by(user_id=m.user_id, role_id=role.id).first()
            if not existing:
                db.session.add(CoreRoleAssignment(organization_id=m.organization_id, user_id=m.user_id, role_id=role.id))
                print(f"Added committee_member role for {m.name}")

    db.session.commit()
    print("Database fixed.")
