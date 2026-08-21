from app import create_app, db
from app.models.auth import AuthSubject

app = create_app()

with app.app_context():
    sace = AuthSubject.query.filter(AuthSubject.slug.in_(['sace', 'cptd'])).all()
    for s in sace:
        print(f"ID: {s.id}, Slug: {s.slug}, Name: {s.name}, Active: {s.is_active}")
