from sqlalchemy import create_engine, text

engine = create_engine('postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db')
with engine.connect() as conn:
    conn.execute(text("ALTER TABLE bil_tenant ADD COLUMN IF NOT EXISTS bank_detail_id INTEGER;"))
    conn.commit()
    print("Added bank_detail_id to bil_tenant.")
