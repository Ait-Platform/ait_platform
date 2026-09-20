with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# 1. Remove overflow-x-auto to prevent the horizontal scroll hiding
text = text.replace('<div class="overflow-x-auto">', '<div class="w-full overflow-hidden">')

# 2. Fix the Table Header to allocate proper space and keep Actions tight
old_thead = """                    <th class="p-4">Group Level</th>
                    <th class="p-4">Seat Title</th>
                    <th class="p-4">System Key</th>
                    <th class="p-4">Assigned Member</th>
                    <th class="p-4 text-right">Actions</th>"""
new_thead = """                    <th class="p-4 w-1/5">Group Level</th>
                    <th class="p-4 w-1/4">Seat Title</th>
                    <th class="p-4 w-1/6">System Key</th>
                    <th class="p-4 w-1/4">Assigned Member</th>
                    <th class="p-4 text-right w-1 whitespace-nowrap">Actions</th>"""
text = text.replace(old_thead, new_thead)

# 3. Update the System Key TD to use wrapping badges + Colors
# We need to replace the old System Key td block
pattern_system_key = r'<td class="p-4 align-middle">\s*<span class="inline-flex items-center px-2 py-1 rounded-full text-\[10px\] font-bold bg-slate-800 text-white shadow-sm uppercase tracking-wider">\s*<i class="fas fa-key mr-1.5 opacity-70"></i> {{ seat\.duty\|default\(\'committee_member\'\)\|replace\(\'_\', \' \'\)\|title }}\s*</span>\s*</td>'

new_system_key = """<td class="p-4 align-middle">
                        {% set d = seat.duty|default('committee_member') %}
                        {% if d == 'manager' or d == 'owner' %}
                        <span class="inline-block px-2 py-1 rounded-lg text-[10px] font-bold bg-rose-600 text-white shadow-sm uppercase tracking-wider leading-tight text-center max-w-[120px]">
                        {% elif d == 'treasurer' or d == 'auditor' %}
                        <span class="inline-block px-2 py-1 rounded-lg text-[10px] font-bold bg-emerald-600 text-white shadow-sm uppercase tracking-wider leading-tight text-center max-w-[120px]">
                        {% elif d == 'committee_member' %}
                        <span class="inline-block px-2 py-1 rounded-lg text-[10px] font-bold bg-indigo-600 text-white shadow-sm uppercase tracking-wider leading-tight text-center max-w-[120px]">
                        {% else %}
                        <span class="inline-block px-2 py-1 rounded-lg text-[10px] font-bold bg-slate-600 text-white shadow-sm uppercase tracking-wider leading-tight text-center max-w-[120px]">
                        {% endif %}
                            <i class="fas fa-key block mb-0.5 opacity-70"></i> {{ d|replace('_', ' ')|title }}
                        </span>
                    </td>"""
text = re.sub(pattern_system_key, new_system_key, text)

# 4. Make sure Actions TD is tightly wrapped
old_actions_td = '<td class="p-4 align-middle text-right space-x-2">'
new_actions_td = '<td class="p-4 align-middle text-right space-x-2 whitespace-nowrap w-1">'
text = text.replace(old_actions_td, new_actions_td)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated table UI for wrapping and colors")
