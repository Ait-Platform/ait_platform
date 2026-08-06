from app import create_app
from app.extensions import db
from sqlalchemy import text
from app.utils.queries import BRIDGE_QUERY

app = create_app()
with app.app_context():
    users = db.session.execute(text("SELECT email FROM \"user\"")).fetchall()
    found = False
    for u in users:
        email = u[0]
        rows = db.session.execute(text(BRIDGE_QUERY), {'email': email}).fetchall()
        for r in rows:
            if r.slug == 'staff':
                print(f"User {email} has staff access_level={r.access_level}")
                found = True
    if not found:
        print("No user has staff in BRIDGE_QUERY")
