import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''    val_invoice = db.session.execute(text(
        "SELECT value FROM system_settings WHERE key = 'mechanic_invoice_cents'")).scalar()
    invoice_cents = int(float(val_invoice)) if val_invoice else 1000
    
    from app.models.billing import TokenTariff
    schedule_tariff = TokenTariff.query.filter_by(program_slug='mechanic', action_name='generate_schedule').first()
    schedule_tokens = schedule_tariff.base_token_cost if schedule_tariff else 10'''

content = re.sub(
    r"    val_invoice = db\.session\.execute\(text\(\s*\"SELECT value FROM system_settings WHERE key = 'mechanic_invoice_cents'\"\)\)\.scalar\(\)\s*invoice_cents = int\(float\(val_invoice\)\) if val_invoice else 1000",
    replacement,
    content,
    flags=re.DOTALL
)

# And inject it into render_template
content = content.replace(
    '''"invoice_cents": invoice_cents,''',
    '''"invoice_cents": invoice_cents,
        "schedule_tokens": schedule_tokens,'''
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

# And use it in the template
with open('templates/program_mechanic/price.html', 'r', encoding='utf-8') as f:
    content2 = f.read()

content2 = content2.replace(
    '''<span class="font-bold text-indigo-600">10.0 Tokens</span>''',
    '''<span class="font-bold text-indigo-600">{{ schedule_tokens | float }} Tokens</span>'''
)

with open('templates/program_mechanic/price.html', 'w', encoding='utf-8') as f:
    f.write(content2)
