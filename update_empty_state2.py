import re

with open('templates/program_mechanic/client_accounts.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''      {% else %}
        <div class="text-center py-12">
          <i class="fas fa-check-circle text-4xl text-green-400 mb-3"></i>
          <h3 class="text-lg font-bold text-slate-700">No Outstanding Balances</h3>
          <p class="text-slate-500 mt-1">None of your clients owe any money for this period. Clients will appear here when their Job Card is approved and unpaid.</p>
        </div>
      {% endif %}'''

content = re.sub(
    r"      \{% else %\}\s*<div class=\"text-center py-12\">\s*<i class=\"fas fa-users text-4xl text-slate-300 mb-3\"></i>\s*<h3 class=\"text-lg font-bold text-slate-700\">No Clients Found</h3>\s*<p class=\"text-slate-500 mt-1\">Clients will automatically appear here when you create Job Cards for them\.</p>\s*</div>\s*\{% endif %\}",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/client_accounts.html', 'w', encoding='utf-8') as f:
    f.write(content)
