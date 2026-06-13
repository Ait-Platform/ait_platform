import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from app import create_app
from app.extensions import db

app = create_app()
with app.app_context():
    is_admin = db.session.execute(db.text("SELECT 1 FROM auth_approved_admin WHERE lower(email)='all@gmail.com'")).scalar()
    print("Is all@gmail.com an admin?", is_admin)
