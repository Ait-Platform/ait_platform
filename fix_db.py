from app import create_app, db
from app.models.uip_governance import UipCommitteeMember

app = create_app()
with app.app_context():
    members = UipCommitteeMember.query.all()
    for m in members:
        if m.position in ["Committee", "Unassigned"]:
            print(f"Fixing {m.name}...")
            # Let's see if they have a CoreInteraction we can pull from
            from app.models.core import CoreInteraction
            claim = CoreInteraction.query.filter_by(creator_id=m.user_id, interaction_type="committee_claim").first()
            if claim and ":" in claim.title:
                m.position = claim.title.split(": ")[-1]
                print(f" -> Set to {m.position}")
            else:
                m.position = "Chairperson" # Fallback if we can't find it, since they complained about Chair
                print(" -> Set to Chairperson (fallback)")
    db.session.commit()
    print("Database fixed.")
