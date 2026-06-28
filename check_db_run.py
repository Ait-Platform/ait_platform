from sqlalchemy import create_engine, text

PG_URL = (
    'postgresql+psycopg2://'
    'ait_platform_db_user:'
    'b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj'
    '@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432'
    '/ait_platform_db'
)
engine = create_engine(PG_URL)

with engine.connect() as conn:
    res = conn.execute(text("""
        SELECT id, run_id, phase_1, phase_2, phase_3, phase_4, total 
        FROM lca_result 
        WHERE run_id = 45
    """)).fetchall()
    print('Results for run 45:', res)
