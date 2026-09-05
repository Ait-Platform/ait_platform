import re

with open('templates/program_practice_crm/pipeline.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_header = """      <div class="flex justify-between items-center mb-6 border-b border-slate-100 pb-4">
        <div>
          <h1 class="text-2xl font-bold text-slate-800">Enquiry Pipeline</h1>
          <p class="text-slate-500">{{ practice.name }}</p>
        </div>
      </div>"""

new_header = """      <div class="flex justify-between items-center mb-6 border-b border-slate-100 pb-4">
        <div>
          <h1 class="text-2xl font-bold text-slate-800">Enquiry Pipeline</h1>
          <p class="text-slate-500">{{ practice.name }}</p>
        </div>
        <div class="flex items-center gap-3">
          <a href="{{ url_for('practice_crm_bp.my_account') }}" class="text-slate-600 hover:text-indigo-700 font-medium px-4 py-2 bg-slate-100 hover:bg-indigo-50 border border-slate-200 rounded transition flex items-center text-sm shadow-sm">
            <i class="fas fa-user-circle mr-2"></i>My Account
          </a>
          <a href="{{ url_for('public_bp.welcome') }}" class="text-slate-500 hover:text-rose-700 font-medium px-4 py-2 border border-slate-200 rounded hover:bg-rose-50 transition text-sm shadow-sm">
            Logout
          </a>
        </div>
      </div>"""

if old_header in content:
    content = content.replace(old_header, new_header)
    with open('templates/program_practice_crm/pipeline.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Injected into pipeline")
else:
    print("Could not find header")
