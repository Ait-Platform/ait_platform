import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    loss_id = db.session.execute(text("SELECT id FROM auth_subject WHERE slug='loss'")).scalar()
    res = db.session.execute(text("SELECT amount_cents, is_active FROM auth_pricing WHERE subject_id=:id"), {"id": loss_id}).fetchall()
    print("Loss pricing rows:", res)
