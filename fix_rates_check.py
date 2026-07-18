import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

old_logic = """      const savedRate = (wizardData.rates && wizardData.rates.find(x => x.account_id === acc.id)) || { amount: 0, charge_to: 'owner', reference: '', erf_details: '', property_category: '', market_value: 0, rateable_value: 0, general_randage: 0, sra_randage: 0, deferred: 0, sra_monthly: 0, general_monthly: 0 };
      
      let cardColor = acc.isBulk ? 'bg-amber-50 border-amber-300' : 'bg-sky-50 border-sky-300';
      let isChecked = (savedRate.amount > 0 || savedRate.market_value > 0);"""

new_logic = """      const rateFound = wizardData.rates && wizardData.rates.find(x => x.account_id === acc.id);
      const savedRate = rateFound || { amount: 0, charge_to: 'owner', reference: '', erf_details: '', property_category: '', market_value: 0, rateable_value: 0, general_randage: 0, sra_randage: 0, deferred: 0, sra_monthly: 0, general_monthly: 0 };
      
      let cardColor = acc.isBulk ? 'bg-amber-50 border-amber-300' : 'bg-sky-50 border-sky-300';
      let isChecked = !!rateFound;"""

if "!!rateFound" not in html:
    html = html.replace(old_logic, new_logic)
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print("Fixed isChecked logic!")
else:
    print("Already fixed!")
