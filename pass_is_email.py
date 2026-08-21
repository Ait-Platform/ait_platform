import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

email_original = '''        # Render the PDF
        html_content = render_template("program_mechanic/invoice_view.html", job_card=job_card, shop=shop)'''

email_new = '''        # Render the PDF
        html_content = render_template("program_mechanic/invoice_view.html", job_card=job_card, shop=shop, is_email=True)'''

content = content.replace(email_original, email_new)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Passed is_email=True to invoice_view in routes.py")
