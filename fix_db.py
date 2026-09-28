from app import create_app, db
from app.models.core import CoreInteraction, CoreOrganizationMember

app = create_app()
with app.app_context():
    claims = CoreInteraction.query.filter_by(interaction_type='mo_claim', status='VERIFIED').all()
    count = 0
    for claim in claims:
        mem = CoreOrganizationMember.query.filter_by(organization_id=claim.organization_id, user_id=claim.creator_id).first()
        if not mem:
            mem = CoreOrganizationMember(organization_id=claim.organization_id, user_id=claim.creator_id, is_active=True)
            db.session.add(mem)
            count += 1
            print(f'Fixed user {claim.creator_id}')
    db.session.commit()
    print(f'Fixed {count} users.')
