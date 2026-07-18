with open('templates/program_billing/checkout_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('{% extends "base.html" %}', '{% extends "layout.html" %}')
text = text.replace("{% extends 'base.html' %}", '{% extends "layout.html" %}')

with open('templates/program_billing/checkout_summary.html', 'w', encoding='utf-8') as f:
    f.write(text)

print('Fixed checkout_summary.html extends statement')
