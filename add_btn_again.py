import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

tracker_btn = '''
          <button onclick="switchTab('tracker')" id="tab-btn-tracker" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px]">
            <span class="mb-1">Repair<br>Tracker</span>
            <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black"><i class="fas fa-search"></i></span>
          </button>'''

regex = r'(<a href="\{\{ url_for\(\'mechanic_bp\.client_accounts\'\) \}\}".*?</a>)'
if 'id="tab-btn-tracker"' not in content:
    content = re.sub(regex, r'\1' + tracker_btn, content, flags=re.DOTALL)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
