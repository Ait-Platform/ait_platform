from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    s = AuthSubject.query.filter_by(slug='budget').first()
    print('requires_price:', s.requires_price, 'commercial_mode:', s.commercial_mode, 'trial_days:', s.trial_days)
