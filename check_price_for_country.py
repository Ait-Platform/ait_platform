import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    subj = AuthSubject.query.filter_by(slug='debtors').first()
    from app.payments.pricing import price_for_country
    local_cents, zar_cents, currency = price_for_country(subj.id, 'ZA')
    print(f"ZA: local_cents={local_cents}, zar_cents={zar_cents}, currency={currency}")
