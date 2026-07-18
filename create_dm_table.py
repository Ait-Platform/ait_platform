from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text('''
            CREATE TABLE IF NOT EXISTS direct_message (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                subject VARCHAR(255) NOT NULL,
                message TEXT NOT NULL,
                reply TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_read BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (user_id) REFERENCES "user" (id) ON DELETE CASCADE
            )
        '''))
        db.session.commit()
        print('DirectMessage table created!')
    except Exception as e:
        print('Error:', e)
