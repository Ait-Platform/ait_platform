import re

html_path = 'templates/program_sace/provider_documents_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

pattern = r'<button onclick="openEmailModal[^>]+>.*?Email\s*</button>'
new_button = '''{% set safe_title = doc.title.replace("'", "").replace('"', '') %}
                        <button onclick="openEmailModal('{{ doc.id }}', '{{ safe_title }}')" class="px-4 py-2 bg-slate-50 text-slate-600 hover:bg-slate-100 hover:text-slate-800 font-bold rounded-lg transition border border-slate-200 shadow-sm flex items-center">
                            <i class="fas fa-envelope mr-2"></i> Email
                        </button>'''

text = re.sub(pattern, new_button, text, flags=re.DOTALL)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
