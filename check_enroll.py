import os
from sqlalchemy import create_engine, text

PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)
engine = create_engine(PG_URL)

with engine.begin() as conn:
    conn.execute(text("""
        DELETE FROM user_enrollment 
        WHERE user_id = (SELECT id FROM "user" WHERE email = 'loss@gmail.com') 
          AND subject_id IN (SELECT id FROM auth_subject WHERE slug IN ('practice_crm', 'hds'))
    """))
    print('Deleted practice_crm and hds enrollments for loss@gmail.com')
