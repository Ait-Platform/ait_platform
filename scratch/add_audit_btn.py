import re

with open('templates/program_sace/compliance/index.html', 'r', encoding='utf-8') as f:
    text = f.read()

btn_html = """    <div class="flex justify-between items-center mb-6">
      <h1 class="text-2xl font-bold text-slate-900">SACE Activity Evaluation Hub</h1>
      <div class="space-x-2">
          <a href="{{ url_for('sace_bp.audit_report') }}" class="px-4 py-2 text-sm font-bold text-white bg-slate-800 rounded-lg hover:bg-slate-700 transition shadow-sm">
            <i class="fas fa-clipboard-list mr-2"></i> Audit Logs
          </a>
          <a href="{{ url_for('public_bp.welcome') }}" class="px-4 py-2 text-sm font-medium text-slate-700 bg-slate-100 rounded-lg hover:bg-slate-200 transition">
            Logout
          </a>
      </div>
    </div>"""

text = re.sub(r'<div class="flex justify-between items-center mb-6">.*?</div>', btn_html, text, flags=re.DOTALL)

with open('templates/program_sace/compliance/index.html', 'w', encoding='utf-8') as f:
    f.write(text)
