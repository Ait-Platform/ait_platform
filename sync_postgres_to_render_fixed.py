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
        print("crm_enquiry alteration ok.")

# 2. Sync auth_subject
with local_engine.connect() as s_conn:
    local_subjects = s_conn.execute(text('SELECT * FROM auth_subject')).mappings().all()

with pg_engine.begin() as p_conn:
    pg_subjects = p_conn.execute(text('SELECT slug FROM auth_subject')).mappings().all()
    pg_slugs = {row['slug'] for row in pg_subjects}
    
    # Insert missing subjects
    inserted = 0
    for s in local_subjects:
        if s['slug'] not in pg_slugs:
            s_dict = dict(s)
            if 'id' in s_dict: del s_dict['id'] # Let PG auto-increment
            # We must use proper inserts, but for simplicity we will do parameterized raw SQL
            cols = list(s_dict.keys())
            vals = [f":{c}" for c in cols]
            sql = f"INSERT INTO auth_subject ({', '.join(cols)}) VALUES ({', '.join(vals)})"
            p_conn.execute(text(sql), s_dict)
            inserted += 1
            print(f"Inserted missing subject: {s['slug']}")
            
    # Update hidden status for all
    for s in local_subjects:
        slug = s['slug']
        is_hidden = bool(s['is_hidden_on_bridge'])
        p_conn.execute(
            text('UPDATE auth_subject SET is_hidden_on_bridge = :h WHERE slug = :s'),
            {"h": is_hidden, "s": slug}
        )
    print(f"Successfully synced auth_subject. Inserted {inserted} new subjects.")

# 3. Mirror subject_country_price
Table("subject_country_price", metadata, autoload_with=local_engine)
metadata.create_all(pg_engine)

with local_engine.connect() as s_conn:
    prices = s_conn.execute(text('SELECT * FROM subject_country_price')).mappings().all()
    local_sub_maps = s_conn.execute(text('SELECT id, slug FROM auth_subject')).mappings().all()
    local_id_to_slug = {r['id']: r['slug'] for r in local_sub_maps}

with pg_engine.connect() as p_conn:
    pg_sub_maps = p_conn.execute(text('SELECT id, slug FROM auth_subject')).mappings().all()
    pg_slug_to_id = {r['slug']: r['id'] for r in pg_sub_maps}

with pg_engine.begin() as p_conn:
    p_conn.execute(text('DELETE FROM subject_country_price'))
    
    fixed_prices = []
    for p in prices:
        p_dict = dict(p)
        
        slug = local_id_to_slug.get(p_dict['subject_id'])
        pg_sid = pg_slug_to_id.get(slug)
        
        if pg_sid:
            p_dict['subject_id'] = pg_sid
            if 'id' in p_dict:
                del p_dict['id']
            fixed_prices.append(p_dict)
        else:
            print(f"Skipping price for unknown local subject id {p_dict['subject_id']}")
    
    if fixed_prices:
        table = metadata.tables["subject_country_price"]
        p_conn.execute(table.insert(), fixed_prices)
        print(f"Successfully mirrored {len(fixed_prices)} rows into subject_country_price.")
    else:
        print("No prices to mirror.")

print("Migration Complete!")
