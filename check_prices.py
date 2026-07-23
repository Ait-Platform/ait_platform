import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject, AuthPricing

app = create_app()
with app.app_context():
    subj = AuthSubject.query.filter_by(slug='debtors').first()
    if subj:
        prices = AuthPricing.query.filter_by(subject_id=subj.id).all()
        for p in prices:
            print(f"{p.currency}: {p.amount_cents}")
        if not prices:
            print("No prices found for debtors")
    else:
        print("Debtors subject not found")
