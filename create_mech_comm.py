from app import create_app
from app.extensions import db
from sqlalchemy import create_engine, text

app = create_app()

print("Updating local database...")
with app.app_context():
    db.session.execute(text('''
        CREATE TABLE IF NOT EXISTS mech_communications (
            id SERIAL PRIMARY KEY,
            job_card_id INTEGER NOT NULL REFERENCES mech_job_cards(id),
            comm_type VARCHAR(50) NOT NULL,
            recipient VARCHAR(150),
            message TEXT,
            status VARCHAR(50) DEFAULT 'Logged',
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    '''))
    db.session.commit()

print("Updating Render database...")
PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)
pg_engine = create_engine(PG_URL)
try:
    with pg_engine.begin() as p_conn:
        p_conn.execute(text('''
            CREATE TABLE IF NOT EXISTS mech_communications (
                id SERIAL PRIMARY KEY,
                job_card_id INTEGER NOT NULL REFERENCES mech_job_cards(id),
                comm_type VARCHAR(50) NOT NULL,
                recipient VARCHAR(150),
                message TEXT,
                status VARCHAR(50) DEFAULT 'Logged',
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        '''))
    print("Successfully updated Render DB.")
except Exception as e:
    print("Render DB error:", e)
