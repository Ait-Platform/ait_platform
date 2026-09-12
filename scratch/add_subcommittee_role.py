from app import create_app
from app.extensions import db
from app.models.core import CoreRole

app = create_app()
with app.app_context():
    if not CoreRole.query.filter_by(slug='subcommittee_member').first():
        role = CoreRole(name='Subcommittee Member', slug='subcommittee_member')
        db.session.add(role)
        db.session.commit()
        print('Added subcommittee_member role.')
    else:
        print('subcommittee_member role already exists.')
