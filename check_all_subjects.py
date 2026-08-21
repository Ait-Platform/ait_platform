from app import create_app, db
from app.models.auth import AuthSubject

app = create_app()

with app.app_context():
    subjects = AuthSubject.query.all()
    for s in subjects:
        if 'sace' in s.slug.lower() or 'cptd' in s.slug.lower() or 'sace' in s.name.lower() or 'cptd' in s.name.lower():
            print(f"ID: {s.id}, Slug: {s.slug}, Name: {s.name}, Active: {s.is_active}")
