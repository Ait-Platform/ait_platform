from sqlalchemy import create_engine, text

LOCAL_URI = 'postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db'
RENDER_URI = 'postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db'

ids_to_delete = [21]

for name, uri in [('Local', LOCAL_URI), ('Render', RENDER_URI)]:
    print(f'Connecting to {name}...')
    try:
        engine = create_engine(uri)
        with engine.begin() as conn:
            conn.execute(text('DELETE FROM adv_math_progress'))
            conn.execute(text('DELETE FROM user_enrollment WHERE subject_id IN (21)'))
            res = conn.execute(text('DELETE FROM auth_subject WHERE id IN (21)'))
            print(f'  Deleted {res.rowcount} subjects from {name}.')
    except Exception as e:
        print(f'  Failed on {name}: {e}')
