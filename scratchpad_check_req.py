import os
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    res = db.session.execute(text("SELECT slug, requires_price FROM auth_subject WHERE slug IN ('home2', 'home_premium', 'home_section3')")).fetchall()
    print("REQUIRES_PRICE:")
    for r in res:
        print(r)
