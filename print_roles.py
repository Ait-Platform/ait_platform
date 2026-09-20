from app import create_app
from app.models.core import CoreRole
try:
    app = create_app()
    with app.app_context():
        roles = CoreRole.query.all()
        for r in roles:
            print(r.id, r.slug, r.name, r.organization_id)
except Exception as e:
    print(e)
