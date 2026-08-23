import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''          <!-- LEFT: Quote & Jobs Tile Block -->
          <div class="flex flex-col gap-2 h-full">
            <a href="{{ url_for('mechanic_bp.new_quote') }}" class="block text-left w-full rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-green-50 border-green-200 hover:border-green-400 group flex-1">
              <div class="font-bold text-slate-900 group-hover:text-green-700 text-lg">Create Quote</div>
              <div class="mt-1 text-sm text-slate-700">Add Customer & Select Parts for a new quote.</div>
            </a>
            <a href="{{ url_for('mechanic_bp.job_cards_list') }}" class="block text-left w-full rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-indigo-50 border-indigo-200 hover:border-indigo-400 group flex-1">
              <div class="font-bold text-slate-900 group-hover:text-indigo-700 text-lg">Recent Job Cards</div>
              <div class="mt-1 text-sm text-slate-700">Track all ongoing jobs, pending quotes, and completed services.</div>
            </a>
          </div>'''

content = re.sub(
    r"<!-- LEFT: Quote & Jobs Tile Block -->\s*<div class=\"flex flex-col gap-2\">\s*<a href=\"\{\{ url_for\('mechanic_bp\.new_quote'\) \}\}\".*?Client Accounts \(SOA\)\s*</a>\s*</div>",
    replacement + '''

          <!-- MIDDLE: Catalog & Debtors Block -->
          <div class="flex flex-col gap-2 h-full">
            <a href="{{ url_for('mechanic_bp.catalog_manage') }}" class="block text-left w-full rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-purple-50 border-purple-200 hover:border-purple-400 group flex-1">
              <div class="font-bold text-slate-900 group-hover:text-purple-700 text-lg">Manage Catalog</div>
              <div class="mt-1 text-sm text-slate-700">Add custom parts and set your local prices.</div>
            </a>
            <a href="{{ url_for('mechanic_bp.client_accounts') }}" class="block text-left w-full rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-sky-50 border-sky-200 hover:border-sky-400 group flex-1">
              <div class="font-bold text-slate-900 group-hover:text-sky-700 text-lg">Client Accounts (SOA)</div>
              <div class="mt-1 text-sm text-slate-700">View balances, generate statements, and manage ledgers.</div>
            </a>
          </div>''',
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
