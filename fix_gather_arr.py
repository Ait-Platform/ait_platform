with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    text = f.read()

old_amount = "amount: parseFloat(card.querySelector('.arr-amount').value) || 0,"
new_amount = "amount: parseAmount(card.querySelector('.arr-amount').value) || 0,"

text = text.replace(old_amount, new_amount)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(text)
print("Updated gatherArrears to use parseAmount!")
