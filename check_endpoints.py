from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    for slug in ['cptd', 'thunee']:
        s = AuthSubject.query.filter_by(slug=slug).first()
        if s:
            print(f"{slug} start_endpoint: {s.start_endpoint}, admin_start_endpoint: {s.admin_start_endpoint}")
