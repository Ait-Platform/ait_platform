import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# 1. Update font sizes in the About section and add the tracker mention
old_about = '''            <p class="text-slate-600 text-sm leading-relaxed mb-4">
                The Archoney Institute of Technology (AIT) presents this submission for SACE endorsement of the <strong>LITRE Blending Machine</strong> reading intervention program. 
            </p>
            <p class="text-slate-600 text-sm leading-relaxed mb-2">
                Due to the proprietary nature of the AIT LITRE Simulator and interactive methodology, standard PDF document review is insufficient. AIT has generated this secure portal to allow you to seamlessly provision SACE Evaluators, generating secure access links for them to evaluate the digital framework firsthand while maintaining intellectual property compliance.
            </p>'''

new_about = '''            <p class="text-slate-600 text-base leading-relaxed mb-4">
                The Archoney Institute of Technology (AIT) presents this submission for SACE endorsement of the <strong>LITRE Blending Machine</strong> reading intervention program. 
            </p>
            <p class="text-slate-600 text-base leading-relaxed mb-2">
                Due to the proprietary nature of the AIT LITRE Simulator and interactive methodology, standard PDF document review is insufficient. AIT has generated this secure portal to allow you to seamlessly provision SACE Evaluators, generating secure access links for them to evaluate the digital framework firsthand while maintaining intellectual property compliance, and an integrated document tracker allowing you to monitor when required forms are viewed or downloaded.
            </p>'''

html = html.replace(old_about, new_about)

# 2. Add the action buttons to the right-aligned row
old_buttons = '''        <div class="flex flex-wrap justify-end gap-3 mb-8">
            {% if has_pledged %}
            <button onclick="openPledgeModal()" class="px-4 py-2 bg-emerald-50 text-emerald-700 hover:bg-emerald-100 font-bold rounded-lg transition border border-emerald-200 shadow-sm"><i class="fas fa-file-signature mr-2"></i> View IP Pledge</button>
            <button onclick="openAddAuditorModal()" class="px-5 py-2.5 bg-indigo-600 text-white hover:bg-indigo-700 font-bold rounded-lg transition shadow-md flex items-center"><i class="fas fa-user-plus mr-2"></i> Provision New Auditor</button>
            {% else %}'''

new_buttons = '''        <div class="flex flex-wrap justify-end gap-3 px-8 mb-8">
            {% if has_pledged %}
            <a href="{{ url_for('sace_bp.provisioning_logs') }}" class="px-4 py-2 bg-slate-50 text-slate-700 hover:bg-slate-100 font-bold rounded-lg transition border border-slate-200 shadow-sm flex items-center"><i class="fas fa-list-alt mr-2"></i> View Audit Logs</a>
            <a href="{{ url_for('sace_bp.provider_documents') }}" class="px-4 py-2 bg-indigo-50 text-indigo-700 hover:bg-indigo-100 font-bold rounded-lg transition border border-indigo-200 shadow-sm flex items-center"><i class="fas fa-folder-open mr-2"></i> Documents</a>
            <button onclick="openPledgeModal()" class="px-4 py-2 bg-emerald-50 text-emerald-700 hover:bg-emerald-100 font-bold rounded-lg transition border border-emerald-200 shadow-sm flex items-center"><i class="fas fa-file-signature mr-2"></i> View IP Pledge</button>
            <button onclick="openAddAuditorModal()" class="px-4 py-2 bg-indigo-600 text-white hover:bg-indigo-700 font-bold rounded-lg transition shadow-md flex items-center"><i class="fas fa-user-plus mr-2"></i> Provision New Auditor</button>
            {% else %}'''

html = html.replace(old_buttons, new_buttons)

# 3. Update IP Pledge modal font sizes
old_pledge = '''<div class="bg-slate-50 border border-slate-200 rounded-xl p-6 text-sm text-slate-700 leading-relaxed space-y-4 shadow-inner mb-6">'''
new_pledge = '''<div class="bg-slate-50 border border-slate-200 rounded-xl p-6 text-base text-slate-700 leading-relaxed space-y-4 shadow-inner mb-6">'''
html = html.replace(old_pledge, new_pledge)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)

print("Provisioning map updated.")
