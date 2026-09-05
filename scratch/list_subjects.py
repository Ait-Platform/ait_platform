from app.extensions import db
from app.models.auth import AuthSubject
from app import create_app

app = create_app()
with app.app_context():
    for s in AuthSubject.query.all():
        print(s.slug, s.name)
