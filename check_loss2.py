from app import create_app
from app.extensions import db
from sqlalchemy import text
import sys

app = create_app()
with app.app_context():
    res = db.session.execute(text("SELECT slug, bypass_dashboard_endpoint FROM auth_subject WHERE slug='loss'")).fetchone()
    print("Loss bypass:", res)
