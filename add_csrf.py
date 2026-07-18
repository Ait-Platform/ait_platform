import re

with open('templates/program_billing/checkout_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace(
    '<form action="{{ url_for(\'billing_bp.billing_unlock\', month=month) }}" method="POST">',
    '<form action="{{ url_for(\'billing_bp.billing_unlock\', month=month) }}" method="POST">\n          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>'
)

with open('templates/program_billing/checkout_summary.html', 'w', encoding='utf-8') as f:
    f.write(text)

print('Added CSRF token to form')
