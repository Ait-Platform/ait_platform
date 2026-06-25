import os
from sqlalchemy import create_engine, MetaData, Table, text

LOCAL_URL = "postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db"
PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)

local_engine = create_engine(LOCAL_URL)
pg_engine = create_engine(PG_URL)

metadata = MetaData()

print("Starting migration to Render Postgres...")

# 1. Alter crm_enquiry table
with pg_engine.begin() as p_conn:
    try:
        p_conn.execute(text('ALTER TABLE crm_enquiry ADD COLUMN medical_aid_plan VARCHAR(100);'))
        print("Successfully added medical_aid_plan to crm_enquiry in PG.")
    except Exception as e:
        print(f"medical_aid_plan column might already exist or error: {e}")

# 2. Update auth_subject hidden status
with local_engine.connect() as s_conn:
    subjects = s_conn.execute(text('SELECT slug, is_hidden_on_bridge FROM auth_subject')).mappings().all()

with pg_engine.begin() as p_conn:
    for s in subjects:
        slug = s['slug']
        is_hidden = bool(s['is_hidden_on_bridge'])
        p_conn.execute(
            text('UPDATE auth_subject SET is_hidden_on_bridge = :h WHERE slug = :s'),
            {"h": is_hidden, "s": slug}
        )
    print("Successfully synced auth_subject is_hidden_on_bridge statuses.")

# 3. Mirror subject_country_price
Table("subject_country_price", metadata, autoload_with=local_engine)
# Make sure it exists on PG
metadata.create_all(pg_engine)

with local_engine.connect() as s_conn:
    prices = s_conn.execute(text('SELECT * FROM subject_country_price')).mappings().all()

with pg_engine.begin() as p_conn:
    # Clear existing prices
    p_conn.execute(text('DELETE FROM subject_country_price'))
    
    fixed_prices = []
    for p in prices:
        p_dict = dict(p)
        # ID is auto-incremented, we can copy exactly as is since subject_ids should match between the two PG DBs
        fixed_prices.append(p_dict)
    
    if fixed_prices:
        table = metadata.tables["subject_country_price"]
        p_conn.execute(table.insert(), fixed_prices)
        print(f"Successfully mirrored {len(fixed_prices)} rows into subject_country_price.")
    else:
        print("No prices to mirror.")

print("Migration Complete!")
