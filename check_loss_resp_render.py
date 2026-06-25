from sqlalchemy import create_engine, text
RENDER_URI = 'postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db'
engine = create_engine(RENDER_URI)
with engine.connect() as conn:
    res = conn.execute(text("SELECT answer, COUNT(*) FROM lca_response WHERE user_id = (SELECT id FROM \"user\" WHERE email='loss!@gmail.com') GROUP BY answer")).fetchall()
    print("Render DB responses:", res)
    runs = conn.execute(text("SELECT id FROM lca_run WHERE user_id = (SELECT id FROM \"user\" WHERE email='loss!@gmail.com') ORDER BY id DESC")).fetchall()
    print("Render DB runs:", runs)
