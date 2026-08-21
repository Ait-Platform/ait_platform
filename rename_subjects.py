import os
from sqlalchemy import create_engine, text

def update_db(url):
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text("UPDATE auth_subject SET name = 'CPTD Workshop' WHERE slug = 'cptd'"))
        conn.execute(text("UPDATE auth_subject SET name = 'SACE Evaluation & Endorsement' WHERE slug = 'sace'"))
        print(f"Updated {url}")

local_url = 'postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db'
render_url = 'postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db'

update_db(local_url)
update_db(render_url)
