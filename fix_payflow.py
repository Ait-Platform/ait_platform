with open('templates/program_adv_math/payflow.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('{{ (reg_cents / 100)|int }}.00', '{{ "%.2f"|format(reg_cents / 100) }}')
content = content.replace('{{ (sub_cents / 100)|int }}', '{{ "%.2f"|format(sub_cents / 100) }}')

with open('templates/program_adv_math/payflow.html', 'w', encoding='utf-8') as f:
    f.write(content)
