import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()

with app.app_context():
    print("Creating all tables locally...")
    db.create_all()
    
    subject = AuthSubject.query.filter_by(slug='debtors').first()
    if not subject:
        subject = AuthSubject(
            slug='debtors',
            name='Debtors & Statements',
            program_type='paid',
            commercial_mode='paid',
            requires_price=1,
            is_active=1
        )
        db.session.add(subject)
        db.session.commit()
        print('Added debtors subject locally.')
    else:
        print('Debtors subject already exists locally.')
