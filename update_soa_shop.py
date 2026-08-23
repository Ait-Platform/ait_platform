import re

with open('app/program_debtors/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Update generate_soa to pass shop
replacement_gen = '''        return render_template('program_debtors/soa_template.html',
                                   debtor=debtor,
                                   ledgers=ledgers,
                                   profile=profile,
                                   running_balance=running_balance,
                                   period_opening_balance=period_opening_balance,
                                   start_date=start_date,
                                   end_date=end_date,
                                   bank_account=bank_account,
                                   shop=shop if 'shop' in locals() else None,
                                   return_url=return_url,
                                   is_pdf=is_pdf_request)'''

content = re.sub(
    r"return render_template\('program_debtors/soa_template\.html',\s*debtor=debtor,\s*ledgers=ledgers,\s*profile=profile,\s*running_balance=running_balance,\s*period_opening_balance=period_opening_balance,\s*start_date=start_date,\s*end_date=end_date,\s*bank_account=bank_account,\s*return_url=return_url,\s*is_pdf=is_pdf_request\)",
    replacement_gen,
    content
)

# Update email_soa to pass shop
replacement_email = '''        html_out = render_template('program_debtors/soa_template.html',
                                   debtor=debtor,
                                   ledgers=ledgers,
                                   profile=profile,
                                   running_balance=running_balance,
                                   period_opening_balance=period_opening_balance,
                                   bank_account=bank_account,
                                   shop=shop if 'shop' in locals() else None,
                                   start_date=start_date,
                                   end_date=end_date,
                                   is_pdf=True)'''

content = re.sub(
    r"html_out = render_template\('program_debtors/soa_template\.html',\s*debtor=debtor,\s*ledgers=ledgers,\s*profile=profile,\s*running_balance=running_balance,\s*period_opening_balance=period_opening_balance,\s*bank_account=bank_account,\s*start_date=start_date,\s*end_date=end_date,\s*is_pdf=True\)",
    replacement_email,
    content
)

with open('app/program_debtors/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
