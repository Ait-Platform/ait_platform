from app import create_app, db
from sqlalchemy import text
app = create_app()
app.app_context().push()
user = db.session.execute(text("SELECT id FROM \"user\" WHERE email='loss2@gmail.com'")).first()
if user:
    print('user_id:', user[0])
    runs = db.session.execute(text('SELECT id, status FROM lca_run WHERE user_id = :uid'), {'uid': user[0]}).fetchall()
    print('runs:', runs)
    for r in runs:
        res = db.session.execute(text('SELECT phase_1, phase_2, phase_3, phase_4, total FROM lca_result WHERE run_id = :rid'), {'rid': r[0]}).first()
        print('run', r[0], 'res:', res)
