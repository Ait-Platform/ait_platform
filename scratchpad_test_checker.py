import os
from flask import Flask, session
from app import create_app
from app.extensions import db

app = create_app()

def test_checker():
    with app.test_request_context('/register/decision?subject=reading', method='GET'):
        # Pretend we are all@gmail.com
        user = db.session.execute(db.text("SELECT id FROM \"user\" WHERE email='all@gmail.com' LIMIT 1")).scalar()
        if not user:
            print("all@gmail.com not found!")
            return

        from flask_login import login_user
        from app.models import User
        login_user(db.session.get(User, user))
        
        session["user_id"] = user
        session["email"] = "all@gmail.com"
        session["quote"] = {
            "country_code": "US",
            "currency": "USD",
            "amount_cents": 2500,
            "zar_amount_cents": 50000,
            "est_zar_cents": 50000,
            "version": "2025-11"
        }
        
        # Make a mock request to see if we get the template
        try:
            from app.auth.routes import register_decision
            resp = register_decision()
            print("Response type:", type(resp))
            if hasattr(resp, 'data'):
                print("Response data snippet:", resp.data[:500].decode('utf-8'))
        except Exception as e:
            print("Error:", e)

if __name__ == "__main__":
    test_checker()
