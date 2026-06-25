from sqlalchemy import create_engine, text
LOCAL_URI = 'postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db'
engine = create_engine(LOCAL_URI)
with engine.connect() as conn:
    print("Executing sum query directly:")
    res = conn.execute(text("""
        SELECT
          SUM(phase_1) AS p1,
          SUM(phase_2) AS p2,
          SUM(phase_3) AS p3,
          SUM(phase_4) AS p4,
          SUM(phase_1+phase_2+phase_3+phase_4) AS total
        FROM (
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
        ) as sub
    """)).fetchall()
    print("  Sum directly:", res)
