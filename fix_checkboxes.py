import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

# Fix Arrears
old_arrears = """      const savedArr = (wizardData.arrears && wizardData.arrears.find(x => x.account_id === acc.id)) || { amount: 0, charge_to: 'owner' };
      
      let cardColor = acc.isBulk ? 'bg-amber-50 border-amber-300' : 'bg-sky-50 border-sky-300';
      let isChecked = savedArr.amount > 0;"""

new_arrears = """      const arrFound = wizardData.arrears && wizardData.arrears.find(x => x.account_id === acc.id);
      const savedArr = arrFound || { amount: 0, charge_to: 'owner' };
      
      let cardColor = acc.isBulk ? 'bg-amber-50 border-amber-300' : 'bg-sky-50 border-sky-300';
      let isChecked = !!arrFound;"""

html = html.replace(old_arrears, new_arrears)

# Fix Arrangements
old_arrangements = """      const savedArg = (wizardData.arrangements && wizardData.arrangements.find(x => x.account_id === acc.id)) || { 
        contract_number: '', charge_to: 'owner', agreement_amount: 0, installments_raised: 0, installment_amount: 0, amount_owing: 0, remaining_periods: 0 
      };
      
      let cardColor = acc.isBulk ? 'bg-amber-50 border-amber-300' : 'bg-sky-50 border-sky-300';
      let isChecked = (savedArg.agreement_amount > 0 || savedArg.contract_number !== '');"""

new_arrangements = """      const argFound = wizardData.arrangements && wizardData.arrangements.find(x => x.account_id === acc.id);
      const savedArg = argFound || { 
        contract_number: '', charge_to: 'owner', agreement_amount: 0, installments_raised: 0, installment_amount: 0, amount_owing: 0, remaining_periods: 0 
      };
      
      let cardColor = acc.isBulk ? 'bg-amber-50 border-amber-300' : 'bg-sky-50 border-sky-300';
      let isChecked = !!argFound;"""

html = html.replace(old_arrangements, new_arrangements)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
print("Fixed Arrears and Arrangements isChecked logic!")
