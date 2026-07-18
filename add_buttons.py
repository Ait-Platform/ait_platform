import re

with open('templates/program_billing/consumption_table.html', 'r', encoding='utf-8') as f:
    text = f.read()

buttons_html = '''
        <div class="flex justify-end space-x-3 mb-6">
          <button onclick="window.print()" class="inline-flex items-center px-4 py-2 bg-slate-100 text-slate-700 text-sm font-semibold rounded-lg hover:bg-slate-200 transition border border-slate-300 shadow-sm">
            <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 17h2a2 2 0 002-2v-4a2 2 0 00-2-2H5a2 2 0 00-2 2v4a2 2 0 002 2h2m2 4h6a2 2 0 002-2v-4a2 2 0 00-2-2H9a2 2 0 00-2 2v4a2 2 0 002 2zm8-12V5a2 2 0 00-2-2H9a2 2 0 00-2 2v4h10z"></path></svg>
            Print
          </button>
          <a href="mailto:?subject=Consumption Review - {{ property.name }} - {{ month }}&body=Please find the consumption review attached." class="inline-flex items-center px-4 py-2 bg-blue-600 text-white text-sm font-semibold rounded-lg hover:bg-blue-700 transition shadow-sm">
            <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"></path></svg>
            Email
          </a>
        </div>
'''

text = re.sub(
    r'(Back to Utilities Hub\s*</a>\s*</div>)',
    r'\1\n' + buttons_html,
    text
)

with open('templates/program_billing/consumption_table.html', 'w', encoding='utf-8') as f:
    f.write(text)

print('Added Print and Email buttons to consumption_table.html')
