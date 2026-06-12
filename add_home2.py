from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    s = AuthSubject.query.filter_by(slug='home2').first()
    if not s:
        s = AuthSubject(slug='home2', name='HOME Section 2', is_active=1, requires_price=1)
        db.session.add(s)
        db.session.commit()
        print('Inserted home2')
    else:
        print('home2 already exists')
