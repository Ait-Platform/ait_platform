import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    res = db.session.execute(text("SELECT id, subject_id, role, plan, amount_cents, is_active FROM auth_pricing")).fetchall()
    for r in res:
        print(r)
