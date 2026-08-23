import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''      <div class="p-6">
        <!-- Row 1: Title and Actions -->
        <div class="flex justify-between items-center mb-6 border-b pb-4">
          <h1 class="text-2xl font-bold text-slate-800">Statement Preview</h1>
          
          <div class="flex items-center gap-3">
            <button onclick="window.print()" class="w-10 h-10 rounded-full bg-slate-100 flex items-center justify-center text-slate-600 hover:bg-slate-200 hover:text-slate-800 transition shadow-sm" title="Print / Save PDF">
              <i class="fas fa-print"></i>
            </button>
            <button onclick="document.getElementById('email-soa-modal').classList.remove('hidden')" class="w-10 h-10 rounded-full bg-blue-50 flex items-center justify-center text-blue-600 hover:bg-blue-100 hover:text-blue-800 transition shadow-sm" title="Email Statement">
              <i class="fas fa-paper-plane"></i>
            </button>
            {% if return_url %}
              <a href="{{ return_url }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
            {% else %}
              <a href="{{ url_for('debtors_bp.debtor_view', debtor_id=debtor.id) }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
            {% endif %}
          </div>
        </div>'''

content = re.sub(
    r"      <div class=\"p-6\">\s*<!-- Row 1: Title and Back button -->.*?<div class=\"flex justify-between items-center mb-4 border-b pb-4\">\s*<h1 class=\"text-2xl font-bold text-slate-800\">Statement Preview</h1>.*?</a>\s*\{% endif %\}\s*</div>\s*<!-- Row 2: Actions -->\s*<div class=\"flex justify-end items-center gap-3 mb-6\">\s*<button onclick=\"window\.print\(\)\".*?</i>\s*</button>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
