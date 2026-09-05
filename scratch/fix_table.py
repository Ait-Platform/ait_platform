import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Remove the "Provision New Auditor" button completely
text = re.sub(r'<button onclick="openAddAuditorModal\(\)".*?</button>', '', text, flags=re.DOTALL)

# Replace the table rows to properly reflect the code, and remove email entirely
table_pattern = r'<div class="overflow-x-auto bg-white rounded-lg border border-slate-200 shadow-sm">.*?</table>'
new_table = '''<div class="overflow-x-auto bg-white rounded-lg border border-slate-200 shadow-sm">
            <table class="w-full text-left border-collapse">
                <thead>
                    <tr class="bg-slate-50 border-b border-slate-200">
                        <th class="p-4 text-xs font-bold uppercase text-slate-500">Access Code</th>
                        <th class="p-4 text-xs font-bold uppercase text-slate-500">Auditor Name</th>
                        <th class="p-4 text-xs font-bold uppercase text-slate-500">Status</th>
                        <th class="p-4 text-xs font-bold uppercase text-slate-500 text-right">Actions</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-slate-100">
                    {% if auditors %}
                        {% for aud in auditors %}
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
                        {% endfor %}
                    {% else %}
                        <tr>
                            <td colspan="4" class="p-8 text-center text-slate-500">
                                <i class="fas fa-user-shield text-3xl text-slate-300 mb-2"></i>
                                <p class="text-sm">No auditors have been provisioned yet.</p>
                            </td>
                        </tr>
                    {% endif %}
                </tbody>
            </table>'''

text = re.sub(table_pattern, new_table, text, flags=re.DOTALL)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
