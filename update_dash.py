import re

with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

nav_buttons = '''
        <!-- Row 2: Dashboard Navigation -->
        <div class="flex justify-end border-b border-slate-200 mb-6 space-x-2">
          <a href="{{ url_for('billing_bp.soa_dashboard') }}" class="px-6 py-3 text-sm font-semibold text-purple-700 bg-purple-50 hover:bg-purple-100 border-b-2 border-purple-600 rounded-t-md transition-colors">
            SOA &rarr;
          </a>
          <a href="{{ url_for('billing_bp.utilities_hub') }}" class="px-6 py-3 text-sm font-semibold text-green-700 bg-green-50 hover:bg-green-100 border-b-2 border-green-600 rounded-t-md transition-colors">
            Utilities Hub &rarr;
          </a>
          <a href="{{ url_for('billing_bp.property_portfolio') }}" class="px-6 py-3 text-sm font-semibold text-blue-700 bg-blue-50 hover:bg-blue-100 border-b-2 border-blue-600 rounded-t-md transition-colors">
            Property Portfolio &rarr;
          </a>
        </div>
'''

text = re.sub(r'<!-- Row 2: Dashboard Navigation -->.*?</div>', nav_buttons, text, flags=re.DOTALL)

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
print('Done modifying manager_dashboard.html')
