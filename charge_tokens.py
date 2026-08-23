import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# I want to add token logic inside accept_quote
# Before we change the status, let's charge the tokens.

regex = r'(\s*job_card = MechJobCard\.query\.get_or_404\(id\)\s*if job_card\.status == \'Quote\':\s*)(job_card\.status = \'Awaiting Deposit\')'

token_logic = '''
        from app.models.auth import AitTokenWallet, AitTokenTransaction
        from sqlalchemy import text
        
        # Charge tokens for generating Tax Invoice
        setting = db.session.execute(text("SELECT value FROM system_settings WHERE key = 'mechanic_quote_cents'")).fetchone()
        quote_cost = int(setting[0]) if setting else 500
        token_cost = quote_cost // 100  # Default 5 tokens, but using same variable to keep it consistent
        
        wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
        if not wallet or wallet.balance < token_cost:
            flash("Insufficient tokens to generate Tax Invoice. Please top up your wallet.", "danger")
            return redirect(url_for("mechanic_bp.mock_bill"))
            
        wallet.balance -= token_cost
        txn = AitTokenTransaction(
            wallet_id=wallet.id,
            amount=-token_cost,
            description=f"Generated and sent Tax Invoice {job_card.job_number}"
        )
        db.session.add(txn)
        
        \\2'''

content = re.sub(regex, token_logic, content)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
