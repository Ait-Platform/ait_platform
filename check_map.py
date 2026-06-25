from sqlalchemy import create_engine, text
LOCAL_URI = 'postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db'
engine = create_engine(LOCAL_URI)
with engine.connect() as conn:
    print("Checking lca_question_phase_map:")
    q_map = conn.execute(text("SELECT * FROM lca_question_phase_map LIMIT 5")).fetchall()
    print("  Map:", q_map)
