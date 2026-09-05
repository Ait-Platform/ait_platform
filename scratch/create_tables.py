from app import create_app
from app.extensions import db
from app.models.auth import FranchiseLicense

app = create_app()
with app.app_context():
    # We only want to create the new table, not overwrite others
    # SQLAlchemy create_all() is safe, it only creates missing tables.
    db.create_all()
    print("Created missing tables locally.")
