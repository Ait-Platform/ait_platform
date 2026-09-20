with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# 1. Update Seat Title to wrap tightly
old_title_td = """                    <td class="p-4 align-middle">
                        <div class="font-bold text-slate-900">{{ seat.title }}</div>
                        {% if seat.qualifier %}
                        <div class="text-[10px] font-bold text-slate-400 uppercase tracking-widest mt-0.5">{{ seat.qualifier }}</div>
                        {% endif %}
                    </td>"""
new_title_td = """                    <td class="p-4 align-middle max-w-[150px] whitespace-normal break-words">
                        <div class="font-bold text-slate-900 leading-tight">{{ seat.title }}</div>
                        {% if seat.qualifier %}
                        <div class="text-[10px] font-bold text-slate-400 uppercase tracking-widest mt-1">{{ seat.qualifier }}</div>
                        {% endif %}
                    </td>"""
text = text.replace(old_title_td, new_title_td)

# 2. Update Actions TD to have text labels
old_actions_td = """                    <td class="p-4 align-middle text-right space-x-2 whitespace-nowrap w-1">
                        <!-- Actions -->
                        <button onclick='openEditSeatModal({{ seat.id }}, `{{ seat.title }}`, `{{ seat.group_level }}`, `{{ seat.qualifier }}`, `{{ seat.duty|default("committee_member") }}`)' class="inline-flex items-center justify-center w-8 h-8 rounded bg-white border border-slate-200 text-slate-500 hover:text-indigo-600 hover:border-indigo-200 hover:bg-indigo-50 shadow-sm transition" title="Edit Seat">
                            <i class="fas fa-pencil-alt text-xs"></i>
                        </button>
                        
                        <button onclick='openAssignModal(`{{ seat.title }}`)' class="inline-flex items-center justify-center w-8 h-8 rounded bg-white border border-slate-200 text-slate-500 hover:text-emerald-600 hover:border-emerald-200 hover:bg-emerald-50 shadow-sm transition" title="Assign Member">
                            <i class="fas fa-user-plus text-xs"></i>
                        </button>

                        {% if seat.member %}
                        <button onclick='openPhotoModal({{ seat.member.id }})' class="inline-flex items-center justify-center w-8 h-8 rounded bg-white border border-slate-200 text-slate-500 hover:text-amber-600 hover:border-amber-200 hover:bg-amber-50 shadow-sm transition" title="Upload Photo">
                            <i class="fas fa-camera text-xs"></i>
                        </button>
                        {% else %}
                        <button disabled class="inline-flex items-center justify-center w-8 h-8 rounded bg-slate-50 border border-slate-100 text-slate-300 cursor-not-allowed" title="Assign a member first">
                            <i class="fas fa-camera text-xs"></i>
                        </button>
                        {% endif %}
                    </td>"""

new_actions_td = """                    <td class="p-4 align-middle text-right whitespace-nowrap w-1">
                        <!-- Actions -->
                        <div class="flex items-center justify-end space-x-2">
                            <button onclick='openEditSeatModal({{ seat.id }}, `{{ seat.title }}`, `{{ seat.group_level }}`, `{{ seat.qualifier }}`, `{{ seat.duty|default("committee_member") }}`)' class="inline-flex items-center px-2 py-1 rounded bg-white border border-slate-200 text-slate-600 hover:text-indigo-700 hover:border-indigo-300 hover:bg-indigo-50 shadow-sm transition text-xs font-bold" title="Edit Seat">
                                <i class="fas fa-pencil-alt mr-1.5 opacity-50"></i> Edit
                            </button>
                            
                            <button onclick='openAssignModal(`{{ seat.title }}`)' class="inline-flex items-center px-2 py-1 rounded bg-white border border-slate-200 text-slate-600 hover:text-emerald-700 hover:border-emerald-300 hover:bg-emerald-50 shadow-sm transition text-xs font-bold" title="Assign Member">
                                <i class="fas fa-user-plus mr-1.5 opacity-50"></i> Assign
                            </button>

                            {% if seat.member %}
                            <button onclick='openPhotoModal({{ seat.member.id }})' class="inline-flex items-center px-2 py-1 rounded bg-white border border-slate-200 text-slate-600 hover:text-amber-700 hover:border-amber-300 hover:bg-amber-50 shadow-sm transition text-xs font-bold" title="Upload Photo">
                                <i class="fas fa-camera mr-1.5 opacity-50"></i> Photo
                            </button>
                            {% else %}
                            <button disabled class="inline-flex items-center px-2 py-1 rounded bg-slate-50 border border-slate-100 text-slate-400 cursor-not-allowed text-xs font-bold" title="Assign a member first">
                                <i class="fas fa-camera mr-1.5 opacity-30"></i> Photo
                            </button>
                            {% endif %}
                        </div>
                    </td>"""

text = text.replace(old_actions_td, new_actions_td)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated Seat Title wrapping and Restored Action Text Labels")
