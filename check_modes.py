import os
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    rows = db.session.execute(text("SELECT slug, commercial_mode, requires_price FROM auth_subject")).mappings().all()
    for r in rows:
        print(f"{r['slug']}: mode={r['commercial_mode']}, requires_price={r['requires_price']}")
