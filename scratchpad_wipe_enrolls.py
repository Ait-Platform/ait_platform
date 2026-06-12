import os
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    # Delete test enrollments for home2, home_premium, home_section3
    db.session.execute(
        text("""
            DELETE FROM user_enrollment 
            WHERE subject_id IN (
                SELECT id FROM auth_subject WHERE slug IN ('home2', 'home_premium', 'home_section3')
            )
            AND user_id IN (
                SELECT id FROM "user" WHERE email IN ('home@gmail.com', 'home1@gmail.com', 'home2@gmail.com')
            )
        """)
    )
    db.session.commit()
    print("Test enrollments wiped successfully!")
