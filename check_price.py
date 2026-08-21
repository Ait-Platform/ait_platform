from app import create_app
from app.extensions import db
from app.models.payment import SubjectCountryPrice
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    subj = AuthSubject.query.filter_by(slug='mechanic').first()
    if subj:
        prices = SubjectCountryPrice.query.filter_by(subject_id=subj.id).all()
        for p in prices:
            print(f"{p.country_code}: {p.currency} {p.local_amount_cents} (ZAR {p.zar_amount_cents})")
