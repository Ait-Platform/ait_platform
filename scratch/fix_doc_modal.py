import re

html_path = 'templates/program_sace/provider_documents_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Replace the button line with a safe version
old_btn = '''<button onclick="openEmailModal('{{ doc.id }}', '{{ doc.title|replace(\"'\", \"\") }}')" class="px-4 py-2 bg-slate-50 text-slate-600 hover:bg-slate-100 hover:text-slate-800 font-bold rounded-lg transition border border-slate-200 shadow-sm flex items-center">'''

new_btn = '''{% set safe_title = doc.title.replace("'", "").replace('"', '') %}
                        <button onclick="openEmailModal('{{ doc.id }}', '{{ safe_title }}')" class="px-4 py-2 bg-slate-50 text-slate-600 hover:bg-slate-100 hover:text-slate-800 font-bold rounded-lg transition border border-slate-200 shadow-sm flex items-center">'''

text = text.replace(old_btn, new_btn)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
