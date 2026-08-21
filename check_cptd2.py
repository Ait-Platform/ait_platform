from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    cptd = AuthSubject.query.filter_by(slug='cptd').first()
    print(cptd.is_hidden_on_bridge)
