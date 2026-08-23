import os
from app import create_app
from app.extensions import db
from app.models.mechanic import MechShop
from app.models.auth import AitTokenWallet, AitTokenTransaction, User

app = create_app()
with app.app_context():
    shops = MechShop.query.filter(MechShop.wallet_balance_cents > 0).all()
    for shop in shops:
        user = User.query.get(shop.user_id)
        if not user:
            continue
            
        cents = shop.wallet_balance_cents
        tokens_to_add = cents // 100
        
        wallet = AitTokenWallet.query.filter_by(user_id=shop.user_id).first()
        if not wallet:
            wallet = AitTokenWallet(user_id=shop.user_id, balance=0)
            db.session.add(wallet)
            db.session.flush()
            
        wallet.balance += tokens_to_add
        
        tx = AitTokenTransaction(
            wallet_id=wallet.id,
            amount=tokens_to_add,
            description=f"Migrated legacy balance ({cents} cents) to {tokens_to_add} tokens"
        )
        db.session.add(tx)
        
        print(f"Migrated {cents} cents to {tokens_to_add} tokens for {user.email}")
        
        # Reset the legacy balance so we don't migrate again
        shop.wallet_balance_cents = 0
        
    db.session.commit()
    print("Migration complete.")
