import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)
engine = create_engine(PG_URL)

with engine.begin() as conn:
    tables = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")).fetchall()
    for (table_name,) in tables:
        try:
            # Postgres sequence names are usually tablename_id_seq
            seq_name = f"{table_name}_id_seq"
            # Check if sequence exists
            seq_exists = conn.execute(text("SELECT 1 FROM pg_class WHERE relkind = 'S' AND relname = :seq_name"), {'seq_name': seq_name}).scalar()
            if seq_exists:
                max_id = conn.execute(text(f'SELECT MAX(id) FROM "{table_name}"')).scalar()
                if max_id is not None:
                    # setval(sequence_name, next_value, is_called)
                    # When is_called is false, the next nextval() call will return the exact value specified.
                    # Usually we want it to be true, so it returns max_id + 1 on next call.
                    conn.execute(text(f"SELECT setval('{seq_name}', {max_id})"))
                    print(f'Reset {seq_name} to max value {max_id}')
        except Exception as e:
            print(f'Error on {table_name}: {e}')
print('Sequences updated!')
