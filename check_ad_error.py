from wsgi import app
from app import db
from app.models.culturalfire import CfiShow, CfiShowAd, CfiWallet
from flask import render_template

with app.test_request_context():
    try:
        user_id = 1
        shows = CfiShow.query.filter_by(status='active').all()
        ads = CfiShowAd.query.filter_by(user_id=user_id).all()
        wallet = CfiWallet.query.filter_by(user_id=user_id).first()
        token_balance = wallet.balance if wallet else 0
        
        # Test rendering
        html = render_template("program_culturefire/ad_dashboard.html", shows=shows, ads=ads, token_balance=token_balance)
        print("Template rendered successfully! Length:", len(html))
    except Exception as e:
        import traceback
        traceback.print_exc()
