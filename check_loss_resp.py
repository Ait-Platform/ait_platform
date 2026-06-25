from sqlalchemy import create_engine, text
LOCAL_URI = 'postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db'
engine = create_engine(LOCAL_URI)
with engine.connect() as conn:
    print("For loss@gmail.com:")
    res = conn.execute(text("SELECT answer, COUNT(*) FROM lca_response WHERE user_id = (SELECT id FROM \"user\" WHERE email='loss@gmail.com') GROUP BY answer")).fetchall()
    print("  Responses:", res)
    runs = conn.execute(text("SELECT id FROM lca_run WHERE user_id = (SELECT id FROM \"user\" WHERE email='loss@gmail.com') ORDER BY id DESC")).fetchall()
    print("  Runs:", runs)
    
    print("For loss1@gmail.com:")
    res1 = conn.execute(text("SELECT answer, COUNT(*) FROM lca_response WHERE user_id = (SELECT id FROM \"user\" WHERE email='loss1@gmail.com') GROUP BY answer")).fetchall()
    print("  Responses:", res1)
    runs1 = conn.execute(text("SELECT id FROM lca_run WHERE user_id = (SELECT id FROM \"user\" WHERE email='loss1@gmail.com') ORDER BY id DESC")).fetchall()
    print("  Runs:", runs1)
