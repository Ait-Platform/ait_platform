
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    uid = 511
    accounts = db.session.execute(text('SELECT id, name, kind, group_label FROM bud_account WHERE user_id = :uid'), {'uid': uid}).mappings().all()
    print('ACCOUNTS:', list(accounts))
    snaps = db.session.execute(text('SELECT id, account_id, arrears_cents FROM bud_snapshot WHERE user_id = :uid'), {'uid': uid}).mappings().all()
    print('SNAPS:', list(snaps))
