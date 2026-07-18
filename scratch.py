from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    db.session.execute(text('CREATE TABLE IF NOT EXISTS cfi_award (id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES "user"(id), earned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'))
    db.session.commit()
    print('Table created successfully')
