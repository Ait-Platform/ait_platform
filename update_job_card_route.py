import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''    from app.models.mechanic import MechShop
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    
    from app.models.debtors import BusinessBankAccount
    bank_account = BusinessBankAccount.query.filter_by(user_id=current_user.id).order_by(BusinessBankAccount.is_default.desc()).first()
    
    return render_template("program_mechanic/job_card.html", job_card=job_card, today_date=today_date, communications=communications, client_debtor=client_debtor, shop=active_shop, bank_account=bank_account)'''

content = content.replace(
    '''    from app.models.mechanic import MechShop
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    return render_template("program_mechanic/job_card.html", job_card=job_card, today_date=today_date, communications=communications, client_debtor=client_debtor, shop=active_shop)''',
    replacement
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
