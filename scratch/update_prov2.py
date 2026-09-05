import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# Move Audit Logs from bottom to top
audit_link_block = '''        <!-- Audit Logs Quick Link -->
        <div class="mt-8 text-center">
            <a href="{{ url_for('sace_bp.audit_report') }}" class="inline-flex items-center px-6 py-3 bg-white border border-slate-200 hover:border-indigo-300 text-slate-600 hover:text-indigo-700 font-bold rounded-lg shadow-sm transition">
                <i class="fas fa-history mr-2"></i> View Platform Audit Logs
            </a>
        </div>'''
html = html.replace(audit_link_block, '')

header_old = '''            <a href="{{ url_for('sace_bp.dashboard') }}" class="mt-4 sm:mt-0 px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200 transition">
                <i class="fas fa-arrow-left mr-1"></i> Back
            </a>
        </div>'''
header_new = '''            <div class="flex flex-col items-end gap-2 mt-4 sm:mt-0">
                <a href="{{ url_for('sace_bp.dashboard') }}" class="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200 transition text-right">
                    <i class="fas fa-arrow-left mr-1"></i> Back
                </a>
                <a href="{{ url_for('sace_bp.audit_report') }}" class="px-4 py-2 text-sm font-bold text-indigo-700 bg-indigo-50 border border-indigo-200 rounded-lg hover:bg-indigo-100 transition text-right">
                    <i class="fas fa-history mr-1"></i> View Platform Audit Logs
                </a>
            </div>
        </div>
        <!-- Audit Log Explainer -->
        <p class="text-xs text-slate-400 mb-6 -mt-2">Use the Platform Audit Logs link above to monitor the real-time activity of your assigned Auditors.</p>
'''
html = html.replace(header_old, header_new)

# Add Explainer Text
explainer_old = '''            <div class="bg-white p-4 rounded border border-indigo-100 mt-4">
                <p class="font-bold text-sm mb-2">How to use this portal:</p>'''
explainer_new = '''            <div class="bg-white p-4 rounded border border-indigo-100 mt-4">
                <p class="text-sm text-slate-700 mb-4 border-b border-indigo-50 pb-3">
                    <i class="fas fa-info-circle text-indigo-400 mr-1"></i> 
                    <strong>Note:</strong> By "Auditors", we are referring to the specific SACE-appointed Evaluators, Endorsement Committee Members, or Subject Matter Experts designated to review this submission.
                </p>
                <p class="font-bold text-sm mb-2">How to use this portal:</p>'''
html = html.replace(explainer_old, explainer_new)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
