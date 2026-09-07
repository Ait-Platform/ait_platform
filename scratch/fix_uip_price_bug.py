import re

routes_path = 'app/uip/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('quote = get_quote_for_subject_country(subject.slug, country_code)', 'quote = get_quote_for_subject_country(subject.id, country_code)')

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
