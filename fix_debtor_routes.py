import sys

with open('app/program_debtors/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix generate_soa
old_gen = '''    html_content = render_template("program_debtors/soa_template.html",
                                   debtor=debtor,
                                   ledgers=period_ledgers,
                                   profile=profile,
                                   running_balance=running_balance,
                                   period_opening_balance=period_opening_balance,
                                   start_date=start_date,
                                   end_date=end_date,
                                   bank_account=bank_account,
                                   return_url=return_url)'''

new_gen = '''    is_pdf_request = (request.args.get('pdf') == '1')
    html_content = render_template("program_debtors/soa_template.html",
                                   debtor=debtor,
                                   ledgers=period_ledgers,
                                   profile=profile,
                                   running_balance=running_balance,
                                   period_opening_balance=period_opening_balance,
                                   start_date=start_date,
                                   end_date=end_date,
                                   bank_account=bank_account,
                                   return_url=return_url,
                                   is_pdf=is_pdf_request)'''
content = content.replace(old_gen, new_gen)


# Fix email_soa
old_email = '''    html_content = render_template("program_debtors/soa_template.html",
                                   debtor=debtor,
                                   ledgers=period_ledgers,
                                   profile=profile,
                                   running_balance=running_balance,
                                   period_opening_balance=period_opening_balance,
                                   bank_account=bank_account,
                                   start_date=start_date,
                                   end_date=end_date)'''

new_email = '''    html_content = render_template("program_debtors/soa_template.html",
                                   debtor=debtor,
                                   ledgers=period_ledgers,
                                   profile=profile,
                                   running_balance=running_balance,
                                   period_opening_balance=period_opening_balance,
                                   bank_account=bank_account,
                                   start_date=start_date,
                                   end_date=end_date,
                                   is_pdf=True)'''
content = content.replace(old_email, new_email)

with open('app/program_debtors/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
