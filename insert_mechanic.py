from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    if not AuthSubject.query.filter_by(slug='mechanic').first():
        mechanic = AuthSubject(
            name="Mechanic CRM",
            slug="mechanic",
            is_active=1
        )
        db.session.add(mechanic)
        db.session.commit()
