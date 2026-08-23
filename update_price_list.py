import re

with open('templates/program_mechanic/price.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''               <div class="flex items-center justify-between bg-white px-4 py-3 rounded-xl shadow-sm border border-slate-100">
                  <span class="text-slate-700 font-medium">Finalize Invoice</span>
                  <span id="invoice_price" class="font-bold text-indigo-600">{{ invoice_cents / 100 }} Tokens</span>
               </div>
               <div class="flex items-center justify-between bg-white px-4 py-3 rounded-xl shadow-sm border border-slate-100">
                  <span class="text-slate-700 font-medium">Debtors Schedule</span>
                  <span class="font-bold text-indigo-600">10.0 Tokens</span>
               </div>'''

content = content.replace(
    '''               <div class="flex items-center justify-between bg-white px-4 py-3 rounded-xl shadow-sm border border-slate-100">
                  <span class="text-slate-700 font-medium">Finalize Invoice</span>
                  <span id="invoice_price" class="font-bold text-indigo-600">{{ invoice_cents / 100 }} Tokens</span>
               </div>''',
    replacement
)

with open('templates/program_mechanic/price.html', 'w', encoding='utf-8') as f:
    f.write(content)
