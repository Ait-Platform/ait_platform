import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''              <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label class="block text-sm font-bold text-slate-700 mb-1">Business Email <span class="text-red-500">*</span></label>
                  <input type="email" name="contact_email" value="{{ active_shop.contact_email if active_shop else current_user.email }}" required class="block w-full rounded-lg border-2 border-slate-200 focus:border-indigo-500 p-2.5">
                </div>
                <div>
                  <label class="block text-sm font-bold text-slate-700 mb-1">Business Phone <span class="text-red-500">*</span></label>
                  <input type="text" name="contact_phone" value="{{ active_shop.contact_phone if active_shop else '' }}" required class="block w-full rounded-lg border-2 border-slate-200 focus:border-indigo-500 p-2.5">
                </div>
              </div>
              
              <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label class="block text-sm font-bold text-slate-700 mb-1">Tax/VAT Number (Optional)</label>
                  <input type="text" name="tax_number" value="{{ active_shop.tax_number if active_shop else '' }}" class="block w-full rounded-lg border-2 border-slate-200 focus:border-indigo-500 p-2.5" placeholder="e.g. 4123456789">
                </div>
                <div>
                  <label class="block text-sm font-bold text-slate-700 mb-1">VAT/Tax Rate (%)</label>
                  <input type="number" step="0.1" min="0" max="100" name="vat_rate" value="{{ active_shop.vat_rate if active_shop else 15.0 }}" class="block w-full rounded-lg border-2 border-slate-200 focus:border-indigo-500 p-2.5">
                  <p class="text-xs text-slate-500 mt-1">Leave as 0 if not registered for VAT.</p>
                </div>
              </div>'''

content = re.sub(
    r"<div class=\"grid grid-cols-1 sm:grid-cols-2 gap-4\">\s*<div>\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">Business Email <span class=\"text-red-500\">\*</span></label>\s*<input type=\"email\" name=\"contact_email\" value=\"\{\{ active_shop\.contact_email if active_shop else current_user\.email \}\}\" required class=\"block w-full rounded-lg border-2 border-slate-200 focus:border-indigo-500 p-2\.5\">\s*</div>\s*<div>\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">Business Phone <span class=\"text-red-500\">\*</span></label>\s*<input type=\"text\" name=\"contact_phone\" value=\"\{\{ active_shop\.contact_phone if active_shop else '' \}\}\" required class=\"block w-full rounded-lg border-2 border-slate-200 focus:border-indigo-500 p-2\.5\">\s*</div>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
