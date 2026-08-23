import re

with open('templates/program_mechanic/client_accounts.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''      <div class="flex items-center gap-3">
        <button onclick="window.print()" class="w-10 h-10 rounded-full bg-slate-100 flex items-center justify-center text-slate-600 hover:bg-slate-200 hover:text-slate-800 transition shadow-sm" title="Print Schedule">
          <i class="fas fa-print"></i>
        </button>
        <a href="{{ url_for('mechanic_bp.mechanic_dashboard') }}" class="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm">
          <span>&larr;</span><span>Dashboard</span>
        </a>
      </div>'''

content = re.sub(
    r"      <div class=\"flex items-center gap-3\">\s*<a href=\"\{\{ url_for\('mechanic_bp\.mechanic_dashboard'\) \}\}\" class=\"inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm\">\s*<span>&larr;</span><span>Dashboard</span>\s*</a>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/client_accounts.html', 'w', encoding='utf-8') as f:
    f.write(content)
