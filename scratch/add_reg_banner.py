import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# Insert the registration banner right after the Row 2 Actions
target_anchor = '''        <div class="flex flex-wrap justify-end gap-3 mb-8">
            {% if has_pledged %}
            <button onclick="openPledgeModal()" class="px-4 py-2 bg-emerald-50 text-emerald-700 hover:bg-emerald-100 font-bold rounded-lg transition border border-emerald-200 shadow-sm">
                <i class="fas fa-check-circle mr-1"></i> IP Pledge Signed
            </button>
            <a href="{{ url_for('sace_bp.audit_report') }}" class="px-4 py-2 bg-slate-800 text-white hover:bg-slate-700 font-bold rounded-lg transition shadow-sm">
                <i class="fas fa-history mr-1"></i> View Audit Logs
            </a>
            {% else %}
            <button onclick="openPledgeModal()" class="px-5 py-2 bg-red-600 text-white hover:bg-red-700 font-bold rounded-lg transition shadow-sm animate-pulse">
                <i class="fas fa-file-signature mr-1"></i> Sign IP Pledge (Required)
            </button>
            {% endif %}
        </div>'''

banner_html = '''
        {% if has_pledged and not current_user.is_authenticated %}
        <!-- Guest Registration Banner -->
        <div class="bg-indigo-50 border border-indigo-200 rounded-xl p-5 mb-8 flex flex-col md:flex-row justify-between items-start md:items-center shadow-sm">
            <div class="mb-4 md:mb-0">
                <h4 class="font-bold text-indigo-900 text-lg"><i class="fas fa-user-shield mr-2 text-indigo-600"></i> Secure Your SACE Control Centre</h4>
                <p class="text-sm text-indigo-800 mt-1 max-w-2xl">You are currently accessing this portal as a guest. Please <strong>register a free account</strong> to save your provisioned auditors and securely access this dashboard across all your devices.</p>
            </div>
            <a href="{{ url_for('auth_bp.register', next=request.path) }}" class="w-full md:w-auto text-center px-6 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow transition whitespace-nowrap md:ml-4">
                Register Account
            </a>
        </div>
        {% endif %}
'''

html = html.replace(target_anchor, target_anchor + banner_html)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
