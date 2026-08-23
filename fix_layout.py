import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# I will just write a simple replacement to fix it up.
bad_html = '''    <!-- First Row: Title & Back Button -->
    <div class="flex items-center justify-between border-b border-slate-100 px-6 pb-4">
      <div>
        <h1 class="text-2xl md:text-3xl font-bold text-slate-900">Recent Job Cards</h1>
      </div>
<div class="px-6">'''

good_html = '''    <!-- First Row: Title & Back Button -->
    <div class="flex items-center justify-between border-b border-slate-100 px-6 pb-4">
      <div>
        <h1 class="text-2xl md:text-3xl font-bold text-slate-900">Recent Job Cards</h1>
      </div>
      <a href="{{ url_for('mechanic_bp.mechanic_dashboard') }}" class="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm">
        <span>&larr;</span><span>Back</span>
      </a>
    </div>
    
<div class="px-6">'''

content = content.replace(bad_html, good_html)

# Now remove the old back button that got pushed down
old_back = '''</div>

      <a href="{{ url_for('mechanic_bp.mechanic_dashboard') }}" class="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm">
        <span>&larr;</span><span>Back</span>
      </a>
    </div>'''

new_back = '''</div>'''

content = content.replace(old_back, new_back)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
