from sqlalchemy import create_engine, text
LOCAL_URI = 'postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db'
engine = create_engine(LOCAL_URI)
with engine.begin() as conn:
    print("Deleting old scorecard...")
    conn.execute(text("DELETE FROM lca_scorecard WHERE run_id = 36"))
    
    print("Inserting into scorecard...")
    res = conn.execute(text("""
        INSERT INTO lca_scorecard
            (user_id, run_id, question_id, answer_type, phase_1, phase_2, phase_3, phase_4)
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
    """))
    print("  Inserted rowcount:", res.rowcount)
