import os
from app import create_app, db
from app.models.auth import AuthSubject

app = create_app()

with app.app_context():
    subjects = AuthSubject.query.filter_by(show_on_welcome=True).all()
    print("Welcome Subjects:")
    for s in subjects:
        print(f"- {s.id}: {s.slug} -> {s.name}")
