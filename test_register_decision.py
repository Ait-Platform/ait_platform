import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app
from app.extensions import db
from app.models.auth import User, AuthSubject
from app.models.payment import VoucherToken

app = create_app()

with app.app_context():
    # Make sure we have a user
    user = User.query.filter_by(email='minor4@test.com').first()
    if not user:
        user = User(name='Minor Four', email='minor4@test.com')
        db.session.add(user)
        db.session.commit()
        
    subj = AuthSubject.query.filter_by(slug='cultural_fire').first()
        
    # Make sure we have a voucher
    vcode = "VOUCHER-TEST-444"
    v = VoucherToken.query.filter_by(code=vcode).first()
    if not v:
        v = VoucherToken(code=vcode, value_amount=150, subject_id=subj.id)
        db.session.add(v)
        db.session.commit()
        
    # simulate login & request
    with app.test_client() as c:
        with c.session_transaction() as sess:
            sess['_user_id'] = str(user.id)
            
        try:
            res = c.get(f'/register/decision?subject=cultural_fire&voucher={vcode}', follow_redirects=False)
            print(f"Register decision status: {res.status_code}")
            if res.status_code == 500:
                print("500 ERROR CAUGHT IN REGISTER_DECISION")
            elif res.status_code == 302:
                print(f"Redirected to: {res.headers.get('Location')}")
        except Exception as e:
            import traceback
            traceback.print_exc()
