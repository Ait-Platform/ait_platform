from sqlalchemy import create_engine, text
LOCAL_URI = 'postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db'
engine = create_engine(LOCAL_URI)
with engine.connect() as conn:
    print("Executing join directly:")
    q = """
        SELECT
            r.user_id,
            r.run_id,
            r.question_id,
            r.answer      AS answer_type,
            m.phase_1,
            m.phase_2,
            m.phase_3,
            m.phase_4
        FROM lca_response r
        JOIN lca_question_phase_map m
          ON m.question_id = r.question_id
         AND m.answer_type = r.answer
        WHERE r.run_id = 36
    """
    res = conn.execute(text(q)).fetchall()
    print("  Join result count:", len(res))
    if len(res) < 5:
        print("  Results:", res)
    else:
        print("  First 5:", res[:5])
