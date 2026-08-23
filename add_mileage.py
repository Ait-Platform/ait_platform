import re

with open('templates/program_mechanic/quote_form.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_year = '''            <div>
              <label class="block text-sm font-medium text-slate-700 mb-1">Year <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
              <input type="number" name="year" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" placeholder="e.g. 2018">
            </div>'''

new_year = '''            <div>
              <label class="block text-sm font-medium text-slate-700 mb-1">Year <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
              <input type="number" name="year" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" placeholder="e.g. 2018">
            </div>
            <div>
              <label class="block text-sm font-medium text-slate-700 mb-1">Mileage (km) <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
              <input type="number" name="mileage" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" placeholder="e.g. 150000">
            </div>'''

content = content.replace(old_year, new_year)

with open('templates/program_mechanic/quote_form.html', 'w', encoding='utf-8') as f:
    f.write(content)
