from app.extensions import db
from app.models.uip_governance import UipOrganogramSeat
from app.models.core import CoreOrganization

def run_migration():
    orgs = CoreOrganization.query.all()
    for org in orgs:
        # Check if the org has the new seats
        existing_seats = [s.title for s in UipOrganogramSeat.query.filter_by(organization_id=org.id).all()]
        new_seats = [
            ("Security Sub-Committee Lead", "SECOND_GROUP", "Voluntary", 5),
            ("Greening & Environment Lead", "SECOND_GROUP", "Voluntary", 6),
            ("Infrastructure & Maintenance Lead", "SECOND_GROUP", "Voluntary", 7),
            ("Social & Community Lead", "SECOND_GROUP", "Voluntary", 8),
            ("Finance & Audit Lead", "SECOND_GROUP", "Voluntary", 9)
        ]
        
        added = 0
        for title, grp, qual, order in new_seats:
            if title not in existing_seats:
                seat = UipOrganogramSeat(organization_id=org.id, title=title, group_level=grp, qualifier=qual, display_order=order)
                db.session.add(seat)
                added += 1
                
        if added > 0:
            db.session.commit()
            print(f"Added {added} missing seats to {org.name}")

if __name__ == "__main__":
    from app import create_app
    app = create_app()
    with app.app_context():
        run_migration()
