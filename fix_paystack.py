import re

with open('app/payments/paystack.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_mech = '''    # ---------- MECHANIC TOPUP ----------
    if subject == "mechanic_topup":
        from app.models.mechanic import MechShop
        shop = MechShop.query.filter_by(user_id=u.id).first()
        if shop and transaction:
            total = int(transaction.get("amount", 0))
            if total > 0:
                shop.wallet_balance_cents += total
                db.session.commit()
        return'''

new_mech = '''    # ---------- MECHANIC TOPUP ----------
    # (Removed intercept so it falls through to Universal Token Topup)'''

content = content.replace(old_mech, new_mech)

with open('app/payments/paystack.py', 'w', encoding='utf-8') as f:
    f.write(content)
