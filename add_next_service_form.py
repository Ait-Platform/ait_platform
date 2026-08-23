import re

with open('templates/program_mechanic/quote_form.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''            <div>
              <label class="block text-sm font-medium text-slate-700 mb-1">Odometer / Mileage <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
              <input type="number" name="mileage" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" value="{{ edit_card.vehicle.mileage if edit_card and edit_card.vehicle else '' }}" placeholder="e.g. 150000">
            </div>
            <div>
              <label class="block text-sm font-medium text-slate-700 mb-1">Next Service Due <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
              <input type="text" name="next_service_due" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" value="{{ edit_card.next_service_due if edit_card else '' }}" placeholder="e.g. 165,000 km or Dec 2026">
            </div>
'''

content = re.sub(
    r"<div>\s*<label class=\"block text-sm font-medium text-slate-700 mb-1\">Odometer / Mileage <span class=\"text-xs text-slate-400 font-normal\">\(Optional\)</span></label>\s*<input type=\"number\" name=\"mileage\" class=\"block\" value=\"\{\{ edit_card\.vehicle\.mileage if edit_card and edit_card\.vehicle else '' \}\}\" w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm\" placeholder=\"e\.g\. 150000\">\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/quote_form.html', 'w', encoding='utf-8') as f:
    f.write(content)
