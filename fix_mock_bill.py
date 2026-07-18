import re

with open('templates/program_billing/checkout_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace("url_for('billing_bp.mock_bill')", "url_for('billing_bp.learner_dashboard')")

with open('templates/program_billing/checkout_summary.html', 'w', encoding='utf-8') as f:
    f.write(text)

print('Fixed mock_bill route')
