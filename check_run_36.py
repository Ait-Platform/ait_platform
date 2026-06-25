from sqlalchemy import create_engine, text
LOCAL_URI = 'postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db'
engine = create_engine(LOCAL_URI)
with engine.connect() as conn:
    print("Checking run_id 36 in lca_response:")
    res = conn.execute(text("SELECT question_id, answer FROM lca_response WHERE run_id=36 LIMIT 5")).fetchall()
    print("  Responses:", res)
    
    print("Checking lca_scorecard for run_id 36:")
    sc = conn.execute(text("SELECT * FROM lca_scorecard WHERE run_id=36 LIMIT 5")).fetchall()
    print("  Scorecard:", sc)
    
    print("Checking lca_result for run_id 36:")
    res_final = conn.execute(text("SELECT * FROM lca_result WHERE run_id=36")).fetchall()
    print("  Result:", res_final)
