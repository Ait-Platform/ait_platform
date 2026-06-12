import os
from flask import Flask, session
from app import create_app
from app.extensions import db

app = create_app()

def test_checker():
    with app.app_context():
        # Find the dummy
        from app.models.auth import User
        user = User.query.filter_by(email="dummy_checker@example.com").first()
        if not user:
            print("Run the previous script to create user first")
            return
        user_id = user.id

    with app.test_request_context('/register/decision?subject=loss', method='GET'):
        from flask_login import login_user
        login_user(db.session.get(User, user_id))
        
        session["user_id"] = user_id
        session["email"] = "dummy_checker@example.com"
        session["reg_ctx"] = {"quote": {
            "country_code": "US",
            "currency": "USD",
            "amount_cents": 2500,
            "zar_amount_cents": 50000,
            "est_zar_cents": 50000,
            "version": "2025-11"
        }}
        
        try:
            from app.auth.routes import register_decision
            resp = register_decision()
            if isinstance(resp, str):
                print("Response contains Account Region Mismatch:", "Account Region Mismatch" in resp)
                print("Response contains Reading:", "Reading" in resp)
            elif hasattr(resp, 'data'):
                print("Response contains Account Region Mismatch:", b"Account Region Mismatch" in resp.data)
        except Exception as e:
            print("Error:", e)

if __name__ == "__main__":
    test_checker()
