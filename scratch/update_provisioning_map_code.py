import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# Replace the "Provision New Auditor" button to just post to generate_code
old_button = '''            <button onclick="openAddAuditorModal()" class="px-4 py-2 bg-indigo-600 text-white hover:bg-indigo-700 font-bold rounded-lg transition shadow-md flex items-center">
                <i class="fas fa-user-plus mr-2"></i> Provision New Auditor
            </button>'''
new_button = '''            <form action="{{ url_for('sace_bp.generate_auditor_code') }}" method="POST" class="inline">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                <button type="submit" class="px-4 py-2 bg-indigo-600 text-white hover:bg-indigo-700 font-bold rounded-lg transition shadow-md flex items-center">
                    <i class="fas fa-ticket-alt mr-2"></i> Generate Access Code
                </button>
            </form>'''
html = html.replace(old_button, new_button)

# Update the table headers
old_thead = '''                    <tr class="bg-slate-50 border-b border-slate-200">
                        <th class="p-4 text-xs font-bold uppercase text-slate-500">Name</th>
                        <th class="p-4 text-xs font-bold uppercase text-slate-500">Email</th>
                        <th class="p-4 text-xs font-bold uppercase text-slate-500">Status</th>
                        <th class="p-4 text-xs font-bold uppercase text-slate-500">Date Provisioned</th>
                        <th class="p-4 text-xs font-bold uppercase text-slate-500 text-right">Actions</th>
                    </tr>'''
new_thead = '''                    <tr class="bg-slate-50 border-b border-slate-200">
                        <th class="p-4 text-xs font-bold uppercase text-slate-500">Access Code</th>
                        <th class="p-4 text-xs font-bold uppercase text-slate-500">Evaluator Name</th>
                        <th class="p-4 text-xs font-bold uppercase text-slate-500">Email</th>
                        <th class="p-4 text-xs font-bold uppercase text-slate-500">Status</th>
                        <th class="p-4 text-xs font-bold uppercase text-slate-500 text-right">Actions</th>
                    </tr>'''
html = html.replace(old_thead, new_thead)

# Update the table rows
old_tbody = '''                        {% for aud in auditors %}
                        <tr class="hover:bg-slate-50 transition">
                            <td class="p-4 text-sm font-bold text-slate-800">{{ aud.first_name }} {{ aud.last_name }}</td>
                            <td class="p-4 text-sm text-slate-600">{{ aud.email }}</td>
                            <td class="p-4 text-sm">
                                <span class="px-2 py-1 rounded text-[10px] font-bold border uppercase tracking-wider {% if 'Completed' in aud.status %}bg-slate-100 text-slate-500 border-slate-200{% elif 'Progress' in aud.status %}bg-blue-50 text-blue-600 border-blue-200{% else %}bg-emerald-50 text-emerald-600 border-emerald-200{% endif %}">
                                    {{ aud.status }}
                                </span>
                            </td>
                            <td class="p-4 text-sm text-slate-500">{{ aud.date }}</td>
                            <td class="p-4 text-right">
                                <div class="flex items-center justify-end space-x-2">
                                    <span class="px-2 py-1 rounded text-[10px] font-bold border uppercase tracking-wider {% if 'Completed' in aud.status %}bg-slate-100 text-slate-500 border-slate-200{% elif 'Progress' in aud.status %}bg-blue-50 text-blue-600 border-blue-200{% else %}bg-emerald-50 text-emerald-600 border-emerald-200{% endif %}">
                                        {{ aud.status }}
                                    </span>
                                </div>
                            </td>
                        </tr>
                        {% endfor %}'''

new_tbody = '''                        {% for aud in auditors %}
                        <tr class="hover:bg-slate-50 transition">
                            <td class="p-4 text-lg font-mono font-bold text-indigo-700 tracking-wider">
                                {% if aud.code %}
                                    {{ aud.code }}
                                {% else %}
                                    <span class="text-slate-400 text-sm italic">Legacy</span>
                                {% endif %}
                            </td>
                            <td class="p-4 text-sm font-bold text-slate-800">
                                {% if aud.first_name %}{{ aud.first_name }} {{ aud.last_name }}{% else %}<span class="text-slate-400 font-normal italic">Pending Claim</span>{% endif %}
                            </td>
                            <td class="p-4 text-sm text-slate-600">
                                {% if aud.email %}{{ aud.email }}{% else %}-{% endif %}
                            </td>
                            <td class="p-4 text-sm">
                                <span class="px-2 py-1 rounded text-[10px] font-bold border uppercase tracking-wider {% if 'Claimed' in aud.status %}bg-emerald-50 text-emerald-600 border-emerald-200{% else %}bg-slate-100 text-slate-500 border-slate-200{% endif %}">
                                    {{ aud.status }}
                                </span>
                            </td>
                            <td class="p-4 text-right">
                                {% if aud.code and 'Unclaimed' in aud.status %}
                                <a href="{{ url_for('sace_bp.print_access_slip', code=aud.code) }}" target="_blank" class="px-3 py-1.5 bg-slate-800 hover:bg-slate-900 text-white font-bold rounded shadow-sm text-xs transition inline-flex items-center">
                                    <i class="fas fa-print mr-2"></i> Print Slip
                                </a>
                                {% else %}
                                <span class="text-xs text-slate-400 italic">No actions</span>
                                {% endif %}
                            </td>
                        </tr>
                        {% endfor %}'''
html = html.replace(old_tbody, new_tbody)

# Remove the Add Auditor Modal and Edit Auditor Modal (since they are now obsolete)
idx_add_start = html.find('<!-- Add Auditor Modal -->')
if idx_add_start != -1:
    idx_add_end = html.find('<!-- Edit Auditor Modal -->')
    if idx_add_end != -1:
        idx_end_final = html.find('</script>', idx_add_end)
        if idx_end_final != -1:
            html = html[:idx_add_start] + html[idx_end_final + 9:]

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
