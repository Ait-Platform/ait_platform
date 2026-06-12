import os
os.add_dll_directory(r'C:\Program Files\GTK3-Runtime Win64\bin')
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    # Insert safely
    sql = text("INSERT INTO auth_subject (slug, name, is_active, sort_order, start_endpoint) VALUES ('spv', 'Special Purpose Vehicles', 1, 90, 'spv_bp.about') ON CONFLICT DO NOTHING")
    db.session.execute(sql)
    db.session.commit()
    print('SPV Added manually')
