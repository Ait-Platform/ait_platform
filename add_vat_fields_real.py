import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''              <div class="grid grid-cols-1 sm:grid-cols-2 gap-6">
                <div class="sm:col-span-2">
                  <label class="block text-sm font-bold text-slate-700 mb-1">Business Name</label>
                  <input type="text" name="business_name" value="{{ active_shop.business_name if active_shop else '' }}" required class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-base p-3 transition">
                </div>
                <div class="sm:col-span-2">
                  <label class="block text-sm font-bold text-slate-700 mb-1">Address</label>
                  <input type="text" name="address" value="{{ active_shop.address if active_shop else '' }}" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-base p-3 transition">
                </div>
                <div>
                  <label class="block text-sm font-bold text-slate-700 mb-1">Phone</label>
                  <input type="text" name="phone" value="{{ active_shop.phone if active_shop else '' }}" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-base p-3 transition">
                </div>
                <div>
                  <label class="block text-sm font-bold text-slate-700 mb-1">Email</label>
                  <input type="email" name="email" value="{{ active_shop.email if active_shop else '' }}" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-base p-3 transition">
                </div>
                
                <!-- VAT Details -->
                <div class="border-t border-slate-200 pt-4 mt-2 sm:col-span-2">
                  <h4 class="text-sm font-bold text-slate-800 mb-4">Tax & VAT Settings</h4>
                  <div class="grid grid-cols-1 sm:grid-cols-2 gap-6">
                    <div>
                      <label class="block text-sm font-bold text-slate-700 mb-1">Tax/VAT Registration No.</label>
                      <input type="text" name="tax_number" value="{{ active_shop.tax_number if active_shop else '' }}" placeholder="e.g. 4123456789" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm p-3 transition">
                    </div>
                    <div>
                      <label class="block text-sm font-bold text-slate-700 mb-1">VAT/Tax Rate (%)</label>
                      <input type="number" step="0.1" min="0" max="100" name="vat_rate" value="{{ active_shop.vat_rate if active_shop else 15.0 }}" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm p-3 transition">
                      <p class="text-xs text-slate-500 mt-1">Default is 15%. Set to 0 if not registered.</p>
                    </div>
                  </div>
                </div>'''

content = re.sub(
    r"<div class=\"grid grid-cols-1 sm:grid-cols-2 gap-6\">\s*<div class=\"sm:col-span-2\">\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">Business Name</label>.*?<div>\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">Email</label>\s*<input type=\"email\" name=\"email\" value=\"\{\{ active_shop\.email if active_shop else '' \}\}\" class=\"block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-base p-3 transition\">\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
