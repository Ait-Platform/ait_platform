import re

with open('templates/program_mechanic/client_accounts.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix button
content = content.replace(
    '''<i class="fas fa-file-invoice-dollar"></i><span>Debtors Schedule (10 Tokens)</span>''',
    '''<i class="fas fa-file-invoice-dollar"></i><span>Debtors Schedule</span>'''
)

# Fix modal text
content = content.replace(
    '''<p class="text-sm text-slate-600 mb-4">Generate a snapshot balance sheet of all your debtors. Generating this report costs <strong class="text-slate-900">10 tokens</strong>.</p>''',
    '''<p class="text-sm text-slate-600 mb-4">Generate a snapshot balance sheet of all your debtors.</p>'''
)

# Fix modal button
content = content.replace(
    '''<button type="submit" class="px-4 py-2 bg-indigo-600 text-white text-sm font-bold rounded-lg hover:bg-indigo-700 shadow-sm transition">Generate (10 Tokens)</button>''',
    '''<button type="submit" class="px-4 py-2 bg-indigo-600 text-white text-sm font-bold rounded-lg hover:bg-indigo-700 shadow-sm transition">Generate Schedule</button>'''
)

with open('templates/program_mechanic/client_accounts.html', 'w', encoding='utf-8') as f:
    f.write(content)
