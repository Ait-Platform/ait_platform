import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    subj = AuthSubject.query.filter_by(slug='debtors').first()
    print(f"Debtors start_endpoint: {subj.start_endpoint}")
    print(f"Debtors about_endpoint: {subj.about_endpoint}")
    print(f"Debtors pay_endpoint: {subj.pay_endpoint}")
