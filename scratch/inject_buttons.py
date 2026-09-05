import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

buttons_html = '''        <!-- Rule 5: Row 2 Actions right-aligned -->
        <div class="flex flex-wrap justify-end gap-3 mb-6 border-b border-slate-100 pb-6">
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
            <form action="{{ url_for('sace_bp.generate_auditor_code') }}" method="POST" class="inline">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                <button type="submit" class="px-4 py-2 bg-indigo-600 text-white hover:bg-indigo-700 font-bold rounded-lg transition shadow-md flex items-center">
                    <i class="fas fa-ticket-alt mr-2"></i> Generate Access Code
                </button>
            </form>
            {% else %}
            <button onclick="openPledgeModal()" class="px-5 py-2.5 bg-red-600 text-white hover:bg-red-700 font-bold rounded-lg transition shadow-md animate-pulse flex items-center">
                <i class="fas fa-file-signature mr-2"></i> Sign IP Pledge (Required)
            </button>
            {% endif %}
        </div>'''

text = text.replace('<!-- Rule 5: Row 2 Actions right-aligned -->', buttons_html)

# Also fix the text: "the LITRE Blending Machine reading intervention program." to the right text.
text = text.replace('the <strong>LITRE Blending Machine</strong> reading intervention program', 'the <strong>I Learn to Read English Using the LITRE Method</strong> intervention program')
text = text.replace('provision SACE Evaluators, generating secure access links for them to evaluate', 'provision SACE Auditors, generating secure access links for them to evaluate')

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
