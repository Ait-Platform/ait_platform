with open('templates/program_billing/architecture_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

import re

# Fix Arrears
text = re.sub(
    r'\{% set has_arrears = false %\}.*?\{% set has_arrears = true %\}.*?\{% endfor %}.*?\{% if has_arrears %\}',
    r'{% set ns_arr = namespace(has=false) %}\n        {% for a in accounts if a.arrears_amount and a.arrears_amount > 0 %}\n            {% set ns_arr.has = true %}\n        {% endfor %}\n        {% if ns_arr.has %}',
    text, flags=re.DOTALL
)

# Fix Rates
text = re.sub(
    r'\{% set has_rates = false %\}.*?\{% set has_rates = true %\}.*?\{% endfor %}.*?\{% if has_rates %\}',
    r'{% set ns_rates = namespace(has=false) %}\n        {% for a in accounts if a.rates_amount and a.rates_amount > 0 %}\n            {% set ns_rates.has = true %}\n        {% endfor %}\n        {% if ns_rates.has %}',
    text, flags=re.DOTALL
)

# Fix Arrangements
text = re.sub(
    r'\{% set has_arrangements = false %\}.*?\{% set has_arrangements = true %\}.*?\{% endfor %}.*?\{% if has_arrangements %\}',
    r'{% set ns_arrg = namespace(has=false) %}\n        {% for a in accounts if a.ca_agreement_amount and a.ca_agreement_amount > 0 %}\n            {% set ns_arrg.has = true %}\n        {% endfor %}\n        {% if ns_arrg.has %}',
    text, flags=re.DOTALL
)

with open('templates/program_billing/architecture_summary.html', 'w', encoding='utf-8') as f:
    f.write(text)
print("Fixed Jinja scoping for Rates, Arrears, and Arrangements!")
