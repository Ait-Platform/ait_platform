import os
from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject
from app.models.payment import SubjectCountryPrice
from datetime import datetime

app = create_app()

with app.app_context():
    # 1. Ensure hds is in auth_subject
    subject = AuthSubject.query.filter_by(slug='hds').first()
    if not subject:
        subject = AuthSubject(
            slug='hds',
            name='Healthcare Data Switch',
            commercial_mode='paid',
            program_type='standalone'
        )
        db.session.add(subject)
        db.session.flush()
        print("Created AuthSubject 'hds'")
    else:
        print("AuthSubject 'hds' already exists")
        
    # 2. Ensure ZA price is 250 ZAR (25000 cents)
    price = SubjectCountryPrice.query.filter_by(subject_id=subject.id, country_code='ZA').first()
    if not price:
        price = SubjectCountryPrice(
            subject_id=subject.id,
            country_code='ZA',
            local_amount_cents=25000,
            zar_amount_cents=25000,
            local_currency='ZAR',
            is_active=True,
            price_version=1,
            created_at=datetime.utcnow()
        )
        db.session.add(price)
        print("Created ZA price for 'hds' (R250.00)")
    else:
        price.local_amount_cents = 25000
        price.zar_amount_cents = 25000
        print("Updated ZA price for 'hds' to R250.00")

    db.session.commit()
    print("Bootstrap complete.")
