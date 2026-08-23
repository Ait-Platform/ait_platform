import re

with open('templates/program_mechanic/client_ledger.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''          <div class="flex items-center gap-2">
            <button onclick="document.getElementById('add-payment-modal').classList.remove('hidden')" class="px-4 py-2 bg-green-600 text-white font-bold rounded-lg hover:bg-green-700 shadow-sm transition text-sm">
              <i class="fas fa-money-bill-wave mr-1"></i> Record Transaction
            </button>
            <a href="{{ url_for('debtors_bp.generate_soa', debtor_id=debtor.id, start_date=start_date, end_date=end_date, return_url=url_for('mechanic_bp.client_ledger', debtor_id=debtor.id)) }}" title="Print / Download SOA" class="w-10 h-10 inline-flex items-center justify-center bg-indigo-100 text-indigo-700 rounded-full hover:bg-indigo-200 transition shadow-sm">
              <i class="fas fa-file-pdf"></i>
            </a>
          </div>'''

content = re.sub(
    r"<div class=\"flex items-center gap-3\">\s*<button onclick=\"document\.getElementById\('add-payment-modal'\)\.classList\.remove\('hidden'\)\" class=\"px-4 py-2 bg-green-600 text-white font-bold rounded-lg hover:bg-green-700 shadow-sm transition text-sm\">\s*<i class=\"fas fa-money-bill-wave mr-1\"></i> Record Transaction\s*</button>\s*<a href=\"\{\{ url_for\('debtors_bp\.generate_soa', debtor_id=debtor\.id, start_date=start_date, end_date=end_date, return_url=url_for\('mechanic_bp\.client_ledger', debtor_id=debtor\.id\)\) \}\}\" class=\"px-4 py-2 bg-indigo-600 text-white font-bold rounded-lg hover:bg-indigo-700 shadow-sm transition text-sm\">\s*<i class=\"fas fa-file-pdf mr-1\"></i> Print / Download SOA\s*</a>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/client_ledger.html', 'w', encoding='utf-8') as f:
    f.write(content)
