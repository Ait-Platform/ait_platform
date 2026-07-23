import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    subjects = AuthSubject.query.all()
    for s in subjects:
        print(f"{s.slug}: {s.start_endpoint}")
