import os
from app import create_app
from app.extensions import db
from app.models.auth import AitTokenWallet, AitTokenTransaction, User

app = create_app()
with app.app_context():
    # Find the user by their most recent activity, or just grant it to all admin/active users
    # We will grant 100 tokens to the first user with an active MechShop
    from app.models.mechanic import MechShop
    active_shops = MechShop.query.all()
    for shop in active_shops:
        user = User.query.get(shop.user_id)
        if not user:
            continue
            
        wallet = AitTokenWallet.query.filter_by(user_id=shop.user_id).first()
        if not wallet:
            wallet = AitTokenWallet(user_id=shop.user_id, balance=0)
            db.session.add(wallet)
            db.session.flush()
            
        wallet.balance += 100
        
        tx = AitTokenTransaction(
            wallet_id=wallet.id,
            amount=100,
            description=f"Manual recovery: Restored 100 tokens from crashed Paystack webhook."
        )
        db.session.add(tx)
        print(f"Granted 100 tokens to {user.email} (Wallet Balance: {wallet.balance})")
        
    db.session.commit()
    print("Token restoration complete.")
