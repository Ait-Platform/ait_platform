import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update update_soa_map to save amounts
old_update_soa = '''    acc.arrears_charge_to = request.form.get("arrears_charge_to", "owner")
    acc.rates_charge_to = request.form.get("rates_charge_to", "owner")
    acc.arrangement_charge_to = request.form.get("arrangement_charge_to", "owner")
    
    db.session.commit()
    flash("SOA Map recorded successfully.", "success")'''

new_update_soa = '''    acc.arrears_charge_to = request.form.get("arrears_charge_to", "owner")
    acc.rates_charge_to = request.form.get("rates_charge_to", "owner")
    acc.arrangement_charge_to = request.form.get("arrangement_charge_to", "owner")
    
    # Save the input amounts
    try:
        acc.rates_amount = float(request.form.get("rates_amount") or 0.0)
    except:
        pass
    try:
        acc.arrears_amount = float(request.form.get("arrears_amount") or 0.0)
    except:
        pass
    try:
        acc.ca_installment_amount = float(request.form.get("ca_installment_amount") or 0.0)
    except:
        pass
        
    db.session.commit()
    flash("SOA Map recorded successfully.", "success")'''

text = text.replace(old_update_soa, new_update_soa)

# 2. Update generate_soa and email_soa to use rates_amount over the calc
old_rates_calc = '''val = round((acc.rates_general_monthly or 0) + (acc.rates_sra_monthly or 0), 2)'''
new_rates_calc = '''val = acc.rates_amount if acc.rates_amount and acc.rates_amount > 0 else round((acc.rates_general_monthly or 0) + (acc.rates_sra_monthly or 0), 2)'''

text = text.replace(old_rates_calc, new_rates_calc)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print("Updated routes.py")
