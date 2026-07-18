from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    db.session.execute(text('CREATE TABLE IF NOT EXISTS cfi_show_ad (id SERIAL PRIMARY KEY, show_id INTEGER REFERENCES cfi_shows(id) NOT NULL, user_id INTEGER REFERENCES "user"(id) NOT NULL, video_url VARCHAR(255) NOT NULL, position_index INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'))
    db.session.commit()
    print('Table cfi_show_ad created successfully')
