import os
from sqlalchemy import create_engine, MetaData, Table, text

# CONFIG
SQLITE_PATH = r"D:\Users\yeshk\Documents\ait_platform\instance\data.db"
SQLITE_URL = f"sqlite:///{SQLITE_PATH}"

PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)

sqlite_engine = create_engine(SQLITE_URL)
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
with sqlite_engine.connect() as s_conn:
    subjects = s_conn.execute(text('SELECT slug, is_hidden_on_bridge FROM auth_subject')).mappings().all()

with pg_engine.begin() as p_conn:
    for s in subjects:
        slug = s['slug']
        # SQLite boolean is 1/0, PG is true/false, SQLAlchemy handles it mostly, but just in case:
        is_hidden = bool(s['is_hidden_on_bridge'])
        p_conn.execute(
            text('UPDATE auth_subject SET is_hidden_on_bridge = :h WHERE slug = :s'),
            {"h": is_hidden, "s": slug}
        )
    print("Successfully synced auth_subject is_hidden_on_bridge statuses.")

# 3. Mirror subject_country_price
Table("subject_country_price", metadata, autoload_with=sqlite_engine)
# Make sure it exists on PG
metadata.create_all(pg_engine)

with sqlite_engine.connect() as s_conn:
    prices = s_conn.execute(text('SELECT * FROM subject_country_price')).mappings().all()
    # Need to remap subject_id from SQLite to PG
    sqlite_subjects = s_conn.execute(text('SELECT id, slug FROM auth_subject')).mappings().all()
    sqlite_id_to_slug = {row["id"]: row["slug"] for row in sqlite_subjects}

with pg_engine.connect() as p_conn:
    pg_subjects = p_conn.execute(text('SELECT id, slug FROM auth_subject')).mappings().all()
    slug_to_pg_id = {row["slug"]: row["id"] for row in pg_subjects}

with pg_engine.begin() as p_conn:
    # Clear existing prices
    p_conn.execute(text('DELETE FROM subject_country_price'))
    
    fixed_prices = []
    for p in prices:
        p_dict = dict(p)
        # Remap subject_id
        slug = sqlite_id_to_slug.get(p_dict['subject_id'])
        pg_sid = slug_to_pg_id.get(slug) if slug else None
        
        if pg_sid:
            p_dict['subject_id'] = pg_sid
            # Remove sqlite specific id so it auto-increments in pg
            if 'id' in p_dict:
                del p_dict['id']
            fixed_prices.append(p_dict)
    
    if fixed_prices:
        table = metadata.tables["subject_country_price"]
        p_conn.execute(table.insert(), fixed_prices)
        print(f"Successfully mirrored {len(fixed_prices)} rows into subject_country_price.")
    else:
        print("No prices to mirror.")

print("Migration Complete!")
