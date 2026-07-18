from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text('''
            CREATE TABLE IF NOT EXISTS system_settings (
                key VARCHAR(255) PRIMARY KEY,
                value VARCHAR(255) NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''))
        db.session.execute(text("INSERT INTO system_settings (key, value) VALUES ('mechanic_quote_cents', '500') ON CONFLICT DO NOTHING"))
        db.session.execute(text("INSERT INTO system_settings (key, value) VALUES ('mechanic_invoice_cents', '1000') ON CONFLICT DO NOTHING"))
        db.session.commit()
        print('System settings table created!')
    except Exception as e:
        print('Error:', e)
