import re

with open('app/program_budget/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''    # --- WALLET TOKEN CHECK ---
    from app.models.billing import TokenTariff
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    
    tariff = TokenTariff.query.filter_by(program_slug='budget', action_name='ledger_entry').first()
    if not tariff:
        tariff = TokenTariff(program_slug='budget', action_name='ledger_entry', base_token_cost=2)
        db.session.add(tariff)
        db.session.commit()
        
    token_cost = tariff.base_token_cost
    
    if not wallet or wallet.balance < token_cost:'''

content = re.sub(
    r"    # --- WALLET TOKEN CHECK ---\s*wallet = AitTokenWallet\.query\.filter_by\(user_id=current_user\.id\)\.first\(\)\s*token_cost = 10\s*if not wallet or wallet\.balance < token_cost:",
    replacement,
    content,
    flags=re.DOTALL
)

with open('app/program_budget/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
