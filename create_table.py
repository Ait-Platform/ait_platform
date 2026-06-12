import psycopg2
conn = psycopg2.connect('postgresql://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db')
c = conn.cursor()
c.execute('CREATE TABLE IF NOT EXISTS cfi_showcase_votes (id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES "user"(id), submission_id INTEGER REFERENCES cfi_talent_submission(id), segment_item_id INTEGER REFERENCES cfi_segment_items(id), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
conn.commit()
print('Table created')
