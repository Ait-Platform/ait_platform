from app.extensions import db
from app import create_app
from sqlalchemy import text
import sys

app = create_app()

with app.app_context():
    # 1. Create the new seat table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS uip_organogram_seat (
        id SERIAL PRIMARY KEY,
        organization_id INTEGER NOT NULL REFERENCES core_organization(id),
        title VARCHAR(100) NOT NULL,
        group_level VARCHAR(50) NOT NULL, -- CORE_EXCO, SECOND_GROUP
        qualifier VARCHAR(50) NOT NULL DEFAULT 'Voluntary', -- Voluntary, Part-Time, Full-Time Paid
        display_order INTEGER DEFAULT 0
    );
    """
    
    # 2. Add seat_id and photo_url to uip_committee_member
    alter_sql_1 = "ALTER TABLE uip_committee_member ADD COLUMN IF NOT EXISTS seat_id INTEGER REFERENCES uip_organogram_seat(id);"
    alter_sql_2 = "ALTER TABLE uip_committee_member ADD COLUMN IF NOT EXISTS photo_url VARCHAR(500);"

    try:
        db.session.execute(text(create_table_sql))
        db.session.execute(text(alter_sql_1))
        db.session.execute(text(alter_sql_2))
        db.session.commit()
        print("Database schema upgraded successfully.")
    except Exception as e:
        db.session.rollback()
        print(f"Error upgrading database: {e}")
        sys.exit(1)
