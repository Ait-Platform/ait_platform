import re

with open('templates/program_mechanic/client_accounts.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''    <!-- First Row: Title & Back Button -->
    <div class="flex flex-col md:flex-row md:items-center justify-between border-b border-slate-100 px-6 pb-4 gap-4">
      <div>
        <h1 class="text-2xl md:text-3xl font-bold text-slate-900">Debtors Control / Client Accounts</h1>
        <p class="mt-1 text-sm text-slate-500">Track all outstanding balances owed by your customers.</p>
      </div>
      <div class="flex items-center gap-3">
        <a href="{{ url_for('mechanic_bp.mechanic_dashboard') }}" class="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm">
          <span>&larr;</span><span>Dashboard</span>
        </a>
      </div>
    </div>
    
    <div class="px-6 py-4 bg-indigo-50 border-b border-slate-100 flex flex-col md:flex-row justify-between items-center gap-4">
        <form method="GET" action="{{ url_for('mechanic_bp.client_accounts') }}" class="flex items-end gap-3 flex-wrap">
            <div>
                <label class="block text-xs font-bold text-slate-600 uppercase mb-1">Start Date</label>
                <input type="date" name="start_date" value="{{ start_date }}" class="rounded-lg border border-slate-300 p-2 text-sm">
            </div>
            <div>
                <label class="block text-xs font-bold text-slate-600 uppercase mb-1">End Date</label>
                <input type="date" name="end_date" value="{{ end_date }}" class="rounded-lg border border-slate-300 p-2 text-sm">
            </div>
            <button type="submit" class="px-4 py-2 bg-indigo-600 text-white font-bold rounded-lg hover:bg-indigo-700 shadow-sm transition text-sm">Filter</button>
            {% if start_date or end_date %}
                <a href="{{ url_for('mechanic_bp.client_accounts') }}" class="px-4 py-2 bg-white text-slate-600 border border-slate-300 font-bold rounded-lg hover:bg-slate-50 shadow-sm transition text-sm">Clear</a>
            {% endif %}
        </form>
        
        <div class="bg-white p-3 rounded-lg border border-indigo-100 shadow-sm text-right">
            <div class="text-xs font-bold text-slate-500 uppercase">Total Outstanding (Period)</div>
            <div class="text-2xl font-bold text-red-600">R {{ "{:,.2f}".format(total_owed) }}</div>
        </div>
    </div>

    <div class="px-6 pt-4 pb-2">
        <p class="text-xs text-slate-500 italic"><i class="fas fa-info-circle mr-1"></i> Disclaimer: This schedule reflects amounts invoiced to clients. It does not deduct the cost of materials or moneys owed to creditors (e.g. parts bought for cash).</p>
    </div>

    <div class="p-6">'''

content = re.sub(
    r"<!-- First Row: Title & Back Button -->\s*<div class=\"flex items-center justify-between border-b border-slate-100 px-6 pb-4\">\s*<div>\s*<h1 class=\"text-2xl md:text-3xl font-bold text-slate-900\">Client Accounts & Ledgers</h1>\s*<p class=\"mt-1 text-sm text-slate-500\">Manage client balances and statements\.</p>\s*</div>\s*<a href=\"\{\{ url_for\('mechanic_bp\.mechanic_dashboard'\) \}\}\" class=\"inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm\">\s*<span>&larr;</span><span>Dashboard</span>\s*</a>\s*</div>\s*<div class=\"p-6\">",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/client_accounts.html', 'w', encoding='utf-8') as f:
    f.write(content)
