import os
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    subjects = ('home2', 'home_premium', 'home_section3')
    for subj in subjects:
        res = db.session.execute(text("SELECT slug, commercial_mode, trial_days FROM auth_subject WHERE slug = :s"), {"s": subj}).fetchall()
        print(f"Before: {res}")
        
    db.session.execute(text("UPDATE auth_subject SET commercial_mode = 'paid' WHERE slug IN ('home2', 'home_premium', 'home_section3')"))
    db.session.commit()
    print("Updated to 'paid'")
    
    for subj in subjects:
        res = db.session.execute(text("SELECT slug, commercial_mode, trial_days FROM auth_subject WHERE slug = :s"), {"s": subj}).fetchall()
        print(f"After: {res}")
