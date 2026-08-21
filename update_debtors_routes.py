import re

with open('app/program_debtors/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Forward return_url from generate_soa to soa_template.html
generate_soa_original = '''    html_content = render_template("program_debtors/soa_template.html",
                                   debtor=debtor,
                                   ledgers=period_ledgers,
                                   profile=profile,
                                   running_balance=running_balance,
                                   period_opening_balance=period_opening_balance,
                                   start_date=start_date,
                                   end_date=end_date,
                                   bank_account=bank_account)'''

generate_soa_new = '''    return_url = request.args.get('return_url')
    html_content = render_template("program_debtors/soa_template.html",
                                   debtor=debtor,
                                   ledgers=period_ledgers,
                                   profile=profile,
                                   running_balance=running_balance,
                                   period_opening_balance=period_opening_balance,
                                   start_date=start_date,
                                   end_date=end_date,
                                   bank_account=bank_account,
                                   return_url=return_url)'''

content = content.replace(generate_soa_original, generate_soa_new)

with open('app/program_debtors/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated debtors routes.py")
