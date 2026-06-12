from app import create_app, db
from sqlalchemy import text
app=create_app()
with app.app_context():
    r=db.session.execute(text("SELECT slug, program_type, commercial_mode, start_endpoint FROM auth_subject WHERE slug='loss'")).mappings().first()
    print(dict(r))
