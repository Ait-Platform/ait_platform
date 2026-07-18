import re

with open('templates/program_billing/input_readings.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Add the 'Generate Statements' block next to the Month Selector
generate_block = '''
      <div class="flex justify-between items-center mb-6">
        <div class="flex items-center p-4 bg-blue-50 border border-blue-200 rounded-xl w-max">
          <label class="text-sm font-bold text-blue-900 mr-4">Select Reading Month:</label>
          <input type="month" id="global_reading_month" class="rounded-md border-2 border-blue-300 p-2 focus:border-blue-500 focus:ring-blue-500 bg-white font-bold text-blue-900" value="{{ current_month }}">
        </div>
        
        <div class="flex items-center p-4 bg-white border border-slate-200 rounded-xl w-max">
          <div class="mr-4">
            <div class="text-sm font-bold text-slate-700">Generate Statements</div>
            <p class="text-xs text-slate-500">Run for <strong><span id="display_month">{{ current_month }}</span></strong></p>
          </div>
          {% if tenant_id %}
            <a id="generateBtn" href="{{ url_for('billing_bp.metsoa', tenant_id=tenant_id, month=current_month) }}" class="bg-slate-800 text-white font-bold py-2 px-4 rounded-lg hover:bg-slate-900 transition block text-center">
              View MetSoa
            </a>
          {% else %}
            <div class="text-sm text-orange-500 italic">No tenants</div>
          {% endif %}
        </div>
      </div>
'''

# Replace the original month selector with the new two-block header
text = re.sub(r'<div class="mb-6 flex items-center p-4 bg-blue-50.*?</div>', generate_block, text, flags=re.DOTALL|re.IGNORECASE)

# Change title
text = text.replace("Input Readings: {{ property.name }}", "Billing & Readings Hub: {{ property.name }}")

# Ensure generate url updates via JS
js_update = '''
      <script>
        document.getElementById('global_reading_month').addEventListener('change', function() {
            var newMonth = this.value;
            // Update all hidden forms
            document.querySelectorAll('.reading-month-input').forEach(function(input) {
                input.value = newMonth;
            });
            
            // Update Generate Statements
            var displaySpan = document.getElementById('display_month');
            if(displaySpan) { displaySpan.innerText = newMonth; }
            
            var generateBtn = document.getElementById('generateBtn');
            if(generateBtn) {
                var urlObj = new URL(generateBtn.href, window.location.origin);
                urlObj.searchParams.set('month', newMonth);
                generateBtn.href = urlObj.pathname + urlObj.search;
            }
        });
'''

# We inject our JS logic
text = text.replace("<script>\n        document.getElementById('global_reading_month').addEventListener('change', function() {\n            var newMonth = this.value;\n            document.querySelectorAll('.reading-month-input').forEach(function(input) {\n                input.value = newMonth;\n            });\n        });", js_update)

with open('templates/program_billing/property_hub.html', 'w', encoding='utf-8') as f:
    f.write(text)

import os
if os.path.exists('templates/program_billing/input_readings.html'):
    os.remove('templates/program_billing/input_readings.html')
print("Done modifying HTML!")
