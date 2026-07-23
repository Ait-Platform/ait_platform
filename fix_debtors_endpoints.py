import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app
from app.models.auth import AuthSubject
from app.extensions import db

app = create_app()
with app.app_context():
    subj = AuthSubject.query.filter_by(slug='debtors').first()
    subj.start_endpoint = 'debtors_bp.debtors_router'
    subj.about_endpoint = 'debtors_bp.about'
    db.session.commit()
    print("Fixed endpoints for debtors")
