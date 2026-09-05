from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    subjects = AuthSubject.query.all()
    for s in subjects:
        print(s.slug, s.name, s.bypass_dashboard_endpoint)
