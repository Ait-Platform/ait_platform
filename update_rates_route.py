import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    routes = f.read()

old_rates = """        # Rates
        rates = data.get('rates', [])
        for r in rates:
            if r.get('account_id') == str(acc.id):
                acc.rates_amount = float(r.get('amount', 0.0))
                acc.rates_date = datetime.strptime(r.get('date'), '%Y-%m-%d').date() if r.get('date') else None
                acc.rates_charge_to = r.get('charge_to', 'owner')"""

new_rates = """        # Rates
        rates = data.get('rates', [])
        for r in rates:
            if r.get('account_id') == str(acc.id):
                acc.rates_amount = float(r.get('amount', 0.0) or 0.0)
                acc.rates_date = datetime.strptime(r.get('date'), '%Y-%m-%d').date() if r.get('date') else None
                acc.rates_charge_to = r.get('charge_to', 'owner')
                acc.rates_reference = r.get('reference', '')
                acc.rates_erf_details = r.get('erf_details', '')
                acc.rates_property_category = r.get('property_category', '')
                acc.rates_market_value = float(r.get('market_value', 0.0) or 0.0)
                acc.rates_rateable_value = float(r.get('rateable_value', 0.0) or 0.0)
                acc.rates_general_randage = float(r.get('general_randage', 0.0) or 0.0)
                acc.rates_sra_randage = float(r.get('sra_randage', 0.0) or 0.0)
                acc.rates_deferred = float(r.get('deferred', 0.0) or 0.0)
                acc.rates_sra_monthly = float(r.get('sra_monthly', 0.0) or 0.0)
                acc.rates_general_monthly = float(r.get('general_monthly', 0.0) or 0.0)"""

if 'rates_market_value' not in routes:
    routes = routes.replace(old_rates, new_rates)
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(routes)
    print("Replaced rates extraction in routes.py")
else:
    print("Already modified.")
