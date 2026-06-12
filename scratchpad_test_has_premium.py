import os
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    has_premium = db.session.scalar(
        text("""
            SELECT 1 FROM user_enrollment 
            WHERE user_id = (SELECT id FROM "user" WHERE email = 'testnew123@gmail.com')
              AND subject_id = (SELECT id FROM auth_subject WHERE slug = 'home2' LIMIT 1)
              AND status IN ('active', 'completed')
            LIMIT 1
        """)
    ) is not None
    print(f"testnew123@gmail.com has_premium: {has_premium}")
    
    has_premium_home1 = db.session.scalar(
        text("""
            SELECT 1 FROM user_enrollment 
            WHERE user_id = (SELECT id FROM "user" WHERE email = 'home1@gmail.com')
              AND subject_id = (SELECT id FROM auth_subject WHERE slug = 'home2' LIMIT 1)
              AND status IN ('active', 'completed')
            LIMIT 1
        """)
    ) is not None
    print(f"home1@gmail.com has_premium: {has_premium_home1}")
