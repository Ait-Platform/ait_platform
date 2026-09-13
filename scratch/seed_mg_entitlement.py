from app import create_app, db
from app.models.core import CoreOrganization, CoreOrganizationEntitlement
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    org = CoreOrganization.query.filter_by(slug='manor-gardens').first()
    subj = AuthSubject.query.filter_by(slug='uip').first()
    
    if org and subj:
        ent = CoreOrganizationEntitlement.query.filter_by(organization_id=org.id, subject_id=subj.id).first()
        if not ent:
            ent = CoreOrganizationEntitlement(
                organization_id=org.id,
                subject_id=subj.id,
                status='complimentary',
                is_trial=True
            )
            db.session.add(ent)
            db.session.commit()
            print("Successfully seeded Manor Gardens UIP entitlement.")
        else:
            print("Entitlement already exists.")
    else:
        print("Org or Subject not found.")
