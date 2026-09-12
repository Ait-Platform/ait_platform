from app import create_app
from app.extensions import db
from app.models.core import CoreRole

app = create_app()
with app.app_context():
    if not CoreRole.query.filter_by(slug='municipal_officer').first():
        role = CoreRole(name='Municipal Officer', slug='municipal_officer')
        db.session.add(role)
        db.session.commit()
        print('Added municipal_officer role.')
    else:
        print('municipal_officer role already exists.')
