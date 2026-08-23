with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''<a href="{{ url_for('debtors_bp.dashboard') }}" class="px-6 py-3 text-sm font-semibold text-purple-700 bg-purple-50 hover:bg-purple-100 border-b-2 border-purple-600 rounded-t-md transition-colors">
            SOA (Debtors) &rarr;
          </a>''',
    '''<a href="{{ url_for('billing_bp.bank_accounts') }}" class="px-6 py-3 text-sm font-semibold text-blue-700 bg-blue-50 hover:bg-blue-100 border-b-2 border-blue-600 rounded-t-md transition-colors">
            <i class="fas fa-university mr-1"></i> Bank Accounts
          </a>
          <a href="{{ url_for('billing_bp.tenant_accounts') }}" class="px-6 py-3 text-sm font-semibold text-purple-700 bg-purple-50 hover:bg-purple-100 border-b-2 border-purple-600 rounded-t-md transition-colors">
            Tenant Accounts (Ledgers)
          </a>'''
)

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
