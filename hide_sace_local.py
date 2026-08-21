import os
from app import create_app, db
from app.models.auth import AuthSubject

app = create_app()

with app.app_context():
    subjects = AuthSubject.query.all()
    for s in subjects:
        if 'sace' in s.name.lower() or 'cptd' in s.name.lower() or 'sace' in s.slug.lower():
            print(f"Hiding on local DB: SLUG: {s.slug}, NAME: {s.name}")
            s.show_on_welcome = False
    
    db.session.commit()
    print("Done hiding subjects on local.")
