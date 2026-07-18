with open('templates/program_billing/soa_map.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Rates
old_rates = '''value="{{ default_rates if default_rates else '' }}"'''
new_rates = '''value="{{ '%.2f'|format(default_rates) if default_rates else '' }}"'''
text = text.replace(old_rates, new_rates)

# Arrears
old_arrears = '''value="{{ selected_account.arrears_amount if selected_account.arrears_amount else '' }}"'''
new_arrears = '''value="{{ '%.2f'|format(selected_account.arrears_amount) if selected_account.arrears_amount else '' }}"'''
text = text.replace(old_arrears, new_arrears)

# Arrangement
old_ca = '''value="{{ selected_account.ca_installment_amount if selected_account.ca_installment_amount else '' }}"'''
new_ca = '''value="{{ '%.2f'|format(selected_account.ca_installment_amount) if selected_account.ca_installment_amount else '' }}"'''
text = text.replace(old_ca, new_ca)

with open('templates/program_billing/soa_map.html', 'w', encoding='utf-8') as f:
    f.write(text)

print('Fixed input formatting to 2 decimals')
