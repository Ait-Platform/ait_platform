import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. download_pdf and email_document both use active_shop
replacement1 = '''    from app.models.debtors import BusinessBankAccount
    bank_account = BusinessBankAccount.query.filter_by(user_id=job_card.vehicle.client.user_id, is_default=True).first()
    
    today_date = datetime.utcnow().strftime('%Y-%m-%d')
    pdf_html_content = render_template("program_mechanic/public_job_card.html", job_card=job_card, shop=active_shop, today_date=today_date, bank_account=bank_account)'''

content = content.replace(
    '''    today_date = datetime.utcnow().strftime('%Y-%m-%d')
    
    pdf_html_content = render_template("program_mechanic/public_job_card.html", job_card=job_card, shop=active_shop, today_date=today_date)''',
    replacement1
)

content = content.replace(
    '''    today_date = datetime.utcnow().strftime('%Y-%m-%d')
        pdf_html_content = render_template("program_mechanic/public_job_card.html", job_card=job_card, shop=active_shop, today_date=today_date)''',
    '''    today_date = datetime.utcnow().strftime('%Y-%m-%d')
        from app.models.debtors import BusinessBankAccount
        bank_account = BusinessBankAccount.query.filter_by(user_id=job_card.vehicle.client.user_id, is_default=True).first()
        pdf_html_content = render_template("program_mechanic/public_job_card.html", job_card=job_card, shop=active_shop, today_date=today_date, bank_account=bank_account)'''
)

# 3. view_public_job_card uses shop
replacement3 = '''    today_date = datetime.utcnow().strftime('%Y-%m-%d')
    from app.models.debtors import BusinessBankAccount
    bank_account = BusinessBankAccount.query.filter_by(user_id=job_card.vehicle.client.user_id, is_default=True).first()
    return render_template('program_mechanic/public_job_card.html', job_card=job_card, shop=shop, today_date=today_date, bank_account=bank_account)'''

content = content.replace(
    '''    today_date = datetime.utcnow().strftime('%Y-%m-%d')
    return render_template('program_mechanic/public_job_card.html', job_card=job_card, shop=shop, today_date=today_date)''',
    replacement3
)


with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
