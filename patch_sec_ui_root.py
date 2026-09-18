import re

with open("templates/program_uip/dashboards/secretary_workspace.html", "r", encoding="utf-8") as f:
    text = f.read()

old_td = """                            <td class="p-4 font-medium text-slate-900">{{ claim.user_name }}</td>
                            <td class="p-4 text-slate-600">{{ claim.user_email }}</td>
                            <td class="p-4">
                                {% if claim.type == 'ratepayer_claim' %}
                                    <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">Ratepayer</span>
                                {% elif claim.type == 'subcommittee_claim' %}
                                    <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800">Subcommittee</span>
                                {% elif claim.type == 'mo_claim' %}
                                    <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-emerald-100 text-emerald-800">Municipal Officer</span>
                                {% elif claim.type == 'staff_claim' %}
                                    <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-slate-100 text-slate-800">Staff / Provider</span>
                                {% else %}
                                    <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">{{ claim.type }}</span>
                                {% endif %}
                            </td>"""

new_td = """                            <td class="p-4 font-medium text-slate-900">{{ claim.user_name }}</td>
                            <td class="p-4 text-slate-600">{{ claim.user_email }}</td>
                            <td class="p-4">
                                <div class="text-sm font-bold text-indigo-700">{{ claim.title }}</div>
                                {% if claim.description %}
                                <div class="text-xs text-slate-500 mt-1 max-w-xs truncate" title="{{ claim.description }}">{{ claim.description }}</div>
                                {% endif %}
                            </td>"""

text = text.replace(old_td, new_td)

with open("templates/program_uip/dashboards/secretary_workspace.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated secretary_workspace.html")

