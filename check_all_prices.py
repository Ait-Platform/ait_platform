import os
from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject
from app.models.payment import SubjectCountryPrice

app = create_app()
with app.app_context():
    print(f"{'Subject':<30} | {'Country':<7} | {'ZAR (cents)':<15} | {'Local Curr':<10} | {'Local Cents':<15}")
    print("-" * 85)
    
    prices = db.session.query(SubjectCountryPrice, AuthSubject).join(AuthSubject, SubjectCountryPrice.subject_id == AuthSubject.id).order_by(AuthSubject.name, SubjectCountryPrice.country_code).all()
    
    for p, s in prices:
        print(f"{s.name[:28]:<30} | {p.country_code:<7} | {p.zar_amount_cents:<15} | {p.local_currency:<10} | {p.local_amount_cents:<15}")
