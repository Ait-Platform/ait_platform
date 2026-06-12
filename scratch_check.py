from sqlalchemy import create_engine, text
db_url = 'postgresql+psycopg2://ait_local:temp1234@localhost:5432/ait_local_db'
engine = create_engine(db_url)
with engine.connect() as conn:
    result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'cfi_talent%'"))
    print('Tables:', [r[0] for r in result.fetchall()])
