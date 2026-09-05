import re
html_path = 'templates/program_sace/compliance/audit_report.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# I will add a Back button to the header
old_header = '''                <div>
                    <h1 class="text-3xl font-black text-slate-800 tracking-tight">System Audit Log</h1>
                    <p class="text-slate-500 mt-1">Immutable record of SACE interactions and access events</p>
                </div>'''

new_header = '''                <div>
                    <h1 class="text-3xl font-black text-slate-800 tracking-tight">System Audit Log</h1>
                    <p class="text-slate-500 mt-1">Immutable record of SACE interactions and access events</p>
                </div>
                {% if is_control_centre %}
                <div>
                    <a href="{{ url_for('sace_bp.provisioning_map') }}" class="px-5 py-2.5 bg-slate-100 hover:bg-slate-200 text-slate-700 font-bold rounded-lg transition border border-slate-300 shadow-sm flex items-center">
                        <i class="fas fa-arrow-left mr-2"></i> Back to Control Centre
                    </a>
                </div>
                {% endif %}'''

html = html.replace(old_header, new_header)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
