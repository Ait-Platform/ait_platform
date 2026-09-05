import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# Locate the buttons section
start_marker = '<div class="flex flex-wrap justify-end gap-3 mb-8">'
end_marker = '{% if not has_pledged %}'

start_idx = html.find(start_marker)
end_idx = html.find(end_marker, start_idx)

new_buttons_html = '''<div class="flex flex-wrap justify-end gap-3 mb-8 px-8 border-b border-slate-100 pb-8">
            {% if has_pledged %}
            <a href="{{ url_for('sace_bp.provisioning_logs') }}" class="px-4 py-2 bg-slate-50 text-slate-700 hover:bg-slate-100 font-bold rounded-lg transition border border-slate-200 shadow-sm flex items-center">
                <i class="fas fa-list-alt mr-2"></i> View Audit Logs
            </a>
            <a href="{{ url_for('sace_bp.provider_documents') }}" class="px-4 py-2 bg-indigo-50 text-indigo-700 hover:bg-indigo-100 font-bold rounded-lg transition border border-indigo-200 shadow-sm flex items-center">
                <i class="fas fa-folder-open mr-2"></i> Provider Documents
            </a>
            <button onclick="openPledgeModal()" class="px-4 py-2 bg-emerald-50 text-emerald-700 hover:bg-emerald-100 font-bold rounded-lg transition border border-emerald-200 shadow-sm flex items-center">
                <i class="fas fa-check-circle mr-2"></i> IP Pledge Signed
            </button>
            <button onclick="openAddAuditorModal()" class="px-4 py-2 bg-indigo-600 text-white hover:bg-indigo-700 font-bold rounded-lg transition shadow-md flex items-center">
                <i class="fas fa-user-plus mr-2"></i> Provision New Auditor
            </button>
            {% else %}
            <button onclick="openPledgeModal()" class="px-5 py-2.5 bg-red-600 text-white hover:bg-red-700 font-bold rounded-lg transition shadow-md animate-pulse flex items-center">
                <i class="fas fa-file-signature mr-2"></i> Sign IP Pledge (Required)
            </button>
            {% endif %}
        </div>
        
        '''

if start_idx != -1 and end_idx != -1:
    html = html[:start_idx] + new_buttons_html + html[end_idx:]

# Also remove the inline 'Provider Submission Documents' block completely
doc_block_start = '<div class="mb-10">'
doc_block_end = '<!-- Auditor Provisioning Section -->'
idx1 = html.find(doc_block_start)
idx2 = html.find(doc_block_end, idx1)

if idx1 != -1 and idx2 != -1:
    html = html[:idx1] + html[idx2:]

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
