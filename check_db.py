from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    rows = db.session.execute(text("SELECT key, value FROM system_settings WHERE key LIKE 'visibility_%'")).fetchall()
    print('Visibility settings in DB:')
    for r in rows:
        print(f'{r.key}: {r.value}')
