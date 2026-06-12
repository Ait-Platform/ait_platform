from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    db.session.execute(text('''
        CREATE TABLE IF NOT EXISTS user_entitlement (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES "user"(id),
            product_slug VARCHAR(255) NOT NULL,
            trial_start TIMESTAMP,
            trial_end TIMESTAMP,
            paid_until TIMESTAMP,
            last_active TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    '''))
    db.session.commit()
    print('Created user_entitlement table')
