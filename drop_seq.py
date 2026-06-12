from sqlalchemy import create_engine, text

engine = create_engine('postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db')
with engine.connect() as conn:
    conn.execute(text("DROP SEQUENCE IF EXISTS bil_bank_detail_id_seq CASCADE;"))
    conn.commit()
    print("Dropped sequence.")
