import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''              <div class="border-t border-slate-200 pt-6 mb-6">
                <label class="block text-sm font-bold text-slate-700 mb-1">Banking Details (For Payments)</label>
                <p class="text-xs text-slate-500 mb-2">These details will appear on your Invoices and Statements.</p>
                <a href="{{ url_for('mechanic_bp.bank_accounts') }}" class="inline-block px-4 py-2 bg-indigo-50 border border-indigo-200 text-indigo-700 font-bold rounded-lg hover:bg-indigo-100 transition shadow-sm w-full text-center">Manage Bank Accounts</a>
              </div>'''

content = re.sub(
    r"<div class=\"border-t border-slate-200 pt-6 mb-6\">\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">Banking Details \(For Payments\)</label>\s*<p class=\"text-xs text-slate-500 mb-2\">These details will appear on your Invoices and Statements\.</p>\s*<textarea name=\"bank_details\" rows=\"3\".*?</textarea>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
