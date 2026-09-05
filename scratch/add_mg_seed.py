@uip_bp.route("/_seed_mg")
def seed_mg():
    from app.extensions import db
    from app.models.core import CoreOrganization, CoreOrganizationWallet
    
    org = CoreOrganization.query.filter_by(slug='manor-gardens').first()
    if not org:
        org = CoreOrganization(
            name='Manor Gardens UIP',
            slug='manor-gardens',
            area='Manor Gardens',
            municipality_ref='eThekwini',
            contact_email='admin@manorgardensuip.co.za',
            contact_phone='031-555-0192',
            status='active'
        )
        db.session.add(org)
        db.session.commit()
    
    wallet = CoreOrganizationWallet.query.filter_by(organization_id=org.id).first()
    if not wallet:
        wallet = CoreOrganizationWallet(organization_id=org.id, balance=1000)
        db.session.add(wallet)
        db.session.commit()
        
    return "Manor Gardens Seeded! <br><br><a href='/uip/manor-gardens/dashboard'>Go to Dashboard</a>"
