from app import create_app, db
from sqlalchemy import text
app = create_app()
with app.app_context():
    from app.models.auth import AuthSubject
    s = AuthSubject.query.filter_by(slug='cultural_fire').first()
    if s:
        # Check if auth_pricing exists
        existing_price = db.session.execute(text("SELECT id FROM auth_pricing WHERE subject_id = :sid"), {'sid': s.id}).first()
        if not existing_price:
            db.session.execute(text("""
                INSERT INTO auth_pricing (subject_id, role, plan, currency, amount_cents, is_active, created_at, updated_at)
                VALUES (:sid, 'user', 'enrollment', 'ZAR', 2000, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """), {'sid': s.id})
            
        # Check if subject_country_price exists
        existing_country_price = db.session.execute(text("SELECT id FROM subject_country_price WHERE subject_id = :sid"), {'sid': s.id}).first()
        if not existing_country_price:
            db.session.execute(text("""
                INSERT INTO subject_country_price (subject_id, country_code, local_currency, local_amount_cents, zar_amount_cents, is_active)
                VALUES (:sid, 'ZA', 'ZAR', 2000, 2000, true)
            """), {'sid': s.id})
            
        db.session.commit()
        print('Successfully added R20 pricing for cultural_fire')
    else:
        print('cultural_fire not found')
