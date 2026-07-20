import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()

with app.app_context():
    subject = AuthSubject.query.filter_by(slug='debtors').first()
    if subject:
        subject.allow_country_pricing = 1
        db.session.commit()
        print('Updated debtors subject to allow country pricing locally.')
