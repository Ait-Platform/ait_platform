import re

file_path = 'templates/program_sace/compliance/index.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# 1. Remove Audit Logs
audit_log_html = '''          <a href="{{ url_for('sace_bp.audit_report') }}" class="px-4 py-2 text-sm font-bold text-white bg-slate-800 rounded-lg hover:bg-slate-700 transition shadow-sm">
            <i class="fas fa-clipboard-list mr-2"></i> Audit Logs
          </a>'''
text = text.replace(audit_log_html, '')

# 2. Change Title to Sace Authorised User(s) Map
text = text.replace('<h1 class="text-2xl font-bold text-slate-900">SACE Activity Evaluation Hub</h1>', '<h1 class="text-2xl font-bold text-slate-900">Sace Authorised User(s) Map</h1>')
text = text.replace('{% block title %}SACE Activity Hub{% endblock %}', '{% block title %}Sace Authorised User(s) Map{% endblock %}')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
