import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Model</label>
              <input type="text" name="model" value="{{ job_card.vehicle.model or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Year</label>
              <input type="number" name="year" value="{{ job_card.vehicle.year or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>'''

content = content.replace(
    '''            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Model</label>
              <input type="text" name="model" value="{{ job_card.vehicle.model or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>''',
    replacement
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
