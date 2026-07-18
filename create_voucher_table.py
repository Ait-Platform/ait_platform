from app import create_app
from app.extensions import db
from sqlalchemy import text
app = create_app()
with app.app_context():
    db.session.execute(text("""
        CREATE TABLE IF NOT EXISTS cfi_voucher (
            id SERIAL PRIMARY KEY,
            code VARCHAR(50) UNIQUE NOT NULL,
            tokens INTEGER DEFAULT 200,
            is_used BOOLEAN DEFAULT FALSE,
            used_by_user_id INTEGER REFERENCES "user"(id),
            used_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    
    # insert a test voucher 'z009'
    db.session.execute(text("INSERT INTO cfi_voucher (code, tokens) VALUES ('z009', 200) ON CONFLICT DO NOTHING"))
    db.session.commit()
    print('Created cfi_voucher table and z009 test voucher directly in database')
