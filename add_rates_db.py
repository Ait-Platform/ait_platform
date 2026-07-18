import re

with open('app/models/billing.py', 'r', encoding='utf-8') as f:
    models = f.read()

new_fields = """    # Financials / Rates / Arrears for this specific account
    rates_reference = db.Column(db.String(100))
    rates_erf_details = db.Column(db.String(255))
    rates_property_category = db.Column(db.String(100))
    rates_market_value = db.Column(db.Float, default=0.0)
    rates_rateable_value = db.Column(db.Float, default=0.0)
    rates_general_randage = db.Column(db.Float, default=0.0)
    rates_sra_randage = db.Column(db.Float, default=0.0)
    rates_deferred = db.Column(db.Float, default=0.0)
    rates_sra_monthly = db.Column(db.Float, default=0.0)
    rates_general_monthly = db.Column(db.Float, default=0.0)

"""

if 'rates_market_value' not in models:
    models = models.replace("    # Financials / Rates / Arrears for this specific account\n", new_fields)
    with open('app/models/billing.py', 'w', encoding='utf-8') as f:
        f.write(models)
    print("Injected new fields into billing.py")
else:
    print("Fields already exist")
