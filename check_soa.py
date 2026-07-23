import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    subj = AuthSubject.query.filter_by(slug='soa').first()
    if subj:
        print(f"SOA found: {subj.name}, start_endpoint: {subj.start_endpoint}")
    else:
        print("SOA not found")
