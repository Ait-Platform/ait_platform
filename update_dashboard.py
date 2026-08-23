import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = re.sub(
    r"<div class=\"bg-blue-50 border-l-4 border-blue-500 p-4 text-sm text-blue-700 m-4 rounded\">\s*<strong>Reminder:</strong>.*?</div>",
    "",
    content,
    flags=re.DOTALL
)

# And update Debtors (SOA) link to Walled Garden
content = re.sub(
    r"<a href=\"\{\{ url_for\('debtors_bp\.dashboard', source='mechanic'\) \}\}\" class=\"block text-center w-full rounded-xl border-2 border-sky-200 p-3 shadow-sm transition hover:shadow bg-sky-50 hover:border-sky-400 text-sky-900 font-semibold text-sm group\">\s*<i class=\"fas fa-file-invoice-dollar mr-1 group-hover:text-sky-700\"></i> Debtors \(SOA\)\s*</a>",
    '''<a href="{{ url_for('mechanic_bp.client_accounts') }}" class="block text-center w-full rounded-xl border-2 border-indigo-200 p-3 shadow-sm transition hover:shadow bg-indigo-50 hover:border-indigo-400 text-indigo-900 font-semibold text-sm group">
              <i class="fas fa-file-invoice-dollar mr-1 group-hover:text-indigo-700"></i> Client Accounts (SOA)
            </a>''',
    content
)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
