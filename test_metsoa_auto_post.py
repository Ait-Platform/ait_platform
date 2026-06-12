from app import create_app, db
from app.models.auth import User
from flask_login import login_user

app = create_app()

with app.app_context():
    app.config['SERVER_NAME'] = 'localhost'
    app.config['WTF_CSRF_ENABLED'] = False
    
    with app.test_client() as client:
        # Mock login
        user = User.query.first()
        if user:
            with client.session_transaction() as sess:
                sess['_user_id'] = str(user.id)
                sess['_fresh'] = True
            
            response = client.get('/metsoa/3/2026-05')
            if response.status_code == 200:
                print("METSOA rendered perfectly!")
                # Also check if ledger entries exist
                from app.models.billing import BilTenantLedger
                entries = BilTenantLedger.query.filter_by(tenant_id=3, month='2026-05').all()
                for e in entries:
                    print(f"Ledger Entry: {e.ref} - {e.amount}")
            else:
                print(f"Failed with status: {response.status_code}")
                print(response.text)
