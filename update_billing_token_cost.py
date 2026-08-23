import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''    from app.models.billing import TokenTariff
    tariff = TokenTariff.query.filter_by(program_slug='billing', action_name='generate_statements').first()
    if not tariff:
        tariff = TokenTariff(program_slug='billing', action_name='generate_statements', base_token_cost=10)
        db.session.add(tariff)
        db.session.commit()
        
    token_cost = meters_billed * tariff.base_token_cost'''

content = re.sub(
    r"    token_cost = meters_billed \* 10",
    replacement,
    content,
    flags=re.DOTALL
)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
