import os

mechanic_dir = 'templates/program_mechanic'
billing_dir = 'templates/program_billing'

# 1. tenant_accounts.html
with open(f'{mechanic_dir}/client_accounts.html', 'r', encoding='utf-8') as f:
    html = f.read()
html = html.replace('mechanic_bp.client_accounts', 'billing_bp.tenant_accounts')
html = html.replace('mechanic_bp.mechanic_dashboard', 'billing_bp.manager_dashboard')
html = html.replace('mechanic_bp.client_ledger', 'billing_bp.tenant_ledger')
html = html.replace('Client Accounts', 'Tenant Accounts')
html = html.replace('Client', 'Tenant')
html = html.replace('ProTrade', 'Billing')
with open(f'{billing_dir}/tenant_accounts.html', 'w', encoding='utf-8') as f:
    f.write(html)

# 2. tenant_ledger.html
with open(f'{mechanic_dir}/client_ledger.html', 'r', encoding='utf-8') as f:
    html = f.read()
html = html.replace('mechanic_bp.client_ledger_add_payment', 'billing_bp.tenant_ledger_add_transaction')
html = html.replace('mechanic_bp.client_ledger', 'billing_bp.tenant_ledger')
html = html.replace('mechanic_bp.client_accounts', 'billing_bp.tenant_accounts')
html = html.replace('Client Ledger', 'Tenant Ledger')
html = html.replace('Client', 'Tenant')
html = html.replace('ProTrade', 'Billing')

# Remove the "Client Job Cards & Tax Invoices" section entirely because billing has no job cards
import re
html = re.sub(
    r"<!-- Job Cards Section -->.*?<!-- Ledger Section -->",
    "<!-- Ledger Section -->",
    html,
    flags=re.DOTALL
)

with open(f'{billing_dir}/tenant_ledger.html', 'w', encoding='utf-8') as f:
    f.write(html)

# 3. bank_accounts.html
with open(f'{mechanic_dir}/bank_accounts.html', 'r', encoding='utf-8') as f:
    html = f.read()
html = html.replace('mechanic_bp', 'billing_bp') # this will fix all bank account routes if I add them to billing_bp, wait I didn't add add_bank_account to billing_bp!
html = html.replace('mechanic_dashboard', 'manager_dashboard')
html = html.replace('ProTrade', 'Billing')
with open(f'{billing_dir}/bank_accounts.html', 'w', encoding='utf-8') as f:
    f.write(html)

