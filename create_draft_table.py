from app import create_app
from app.extensions import db
from app.models.billing import BilArchitectureDraft

app = create_app()
with app.app_context():
    # Because we added a new model, we just run create_all
    # It will safely ignore existing tables and only create missing ones.
    db.create_all()
    print("Database tables ensured.")
