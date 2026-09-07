import re

html_path = 'templates/uip/price.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('{{ quote.currency_symbol }}{{ "%.2f"|format(quote.price) }}', '{{ quote.local_currency }} {{ "%.2f"|format(quote.local_amount_cents / 100) }}')

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
