import os
from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    subjects = db.session.query(AuthSubject).all()
    for s in subjects:
        if s.slug in ['spv', 'grade_12_math', 'admin', 'admin_general']:
            s.is_hidden_on_bridge = True
        else:
            s.is_hidden_on_bridge = False
    
    db.session.commit()
    print("Launch status updated successfully.")
    
    # Verify
    updated = db.session.query(AuthSubject).all()
    print(f"{'ID':<4} | {'Slug':<20} | {'Name':<35} | {'Hidden':<6}")
    for s in updated:
        print(f"{s.id:<4} | {s.slug:<20} | {s.name:<35} | {s.is_hidden_on_bridge}")
