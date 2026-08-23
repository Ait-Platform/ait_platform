import re

with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''          <!-- Row 2: Dashboard Navigation -->
          <div class="flex justify-end border-b border-slate-200 mb-6 space-x-2">
            <a href="{{ url_for('billing_bp.bank_accounts') }}" class="px-6 py-3 text-sm font-semibold text-blue-700 bg-blue-50 hover:bg-blue-100 border-b-2 border-blue-600 rounded-t-md transition-colors">
              <i class="fas fa-university mr-1"></i> Bank Accounts
            </a>
            <a href="{{ url_for('billing_bp.tenant_accounts') }}" class="px-6 py-3 text-sm font-semibold text-purple-700 bg-purple-50 hover:bg-purple-100 border-b-2 border-purple-600 rounded-t-md transition-colors">
              Tenant Accounts (Ledgers)
            </a>'''

content = re.sub(
    r"          <!-- Row 2: Dashboard Navigation -->\s*<div class=\"flex justify-end border-b border-slate-200 mb-6 space-x-2\">\s*<a href=\"\{\{ url_for\('billing_bp\.tenant_accounts'\) \}\}\" class=\"px-6 py-3 text-sm font-semibold text-purple-700 bg-purple-50 hover:bg-purple-100 border-b-2 border-purple-600 rounded-t-md transition-colors\">\s*Tenant Accounts \(Ledgers\)\s*</a>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
