import os
from app import create_app
from app.extensions import db
from sqlalchemy import inspect

app = create_app()
with app.app_context():
    inspector = inspect(db.engine)
    tables = inspector.get_table_names()
    if 'user_entitlement' in tables:
        print("YES, user_entitlement exists.")
    else:
        print("NO, user_entitlement does NOT exist.")
