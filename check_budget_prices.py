from app import create_app
from app.extensions import db
from app.models.payment import SubjectCountryPrice
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    s = AuthSubject.query.filter_by(slug='budget').first()
    if s:
        prices = SubjectCountryPrice.query.filter_by(subject_id=s.id).all()
        print('Budget Prices:', [(p.country_code, p.local_amount_cents) for p in prices])
    else:
        print('budget subject not found')
