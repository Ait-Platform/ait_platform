from app import create_app
from app.extensions import db
from sqlalchemy import text
from app.utils.queries import BRIDGE_QUERY
app = create_app()
with app.app_context():
    u = db.session.execute(text("SELECT email FROM \"user\" JOIN crm_practice_user pu ON pu.user_id = \"user\".id LIMIT 1")).scalar()
    if u:
        print('Testing with CRM user:', u)
        rows = db.session.execute(text(BRIDGE_QUERY), {'email': u}).fetchall()
        for r in rows:
            print(r.slug, r.access_level)
    else:
        print('No CRM user found')
