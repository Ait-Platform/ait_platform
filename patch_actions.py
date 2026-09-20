with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

old_actions = """                    <td class="p-4 align-middle text-right space-x-2">
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

new_actions = """                    <td class="p-4 align-middle text-right">
                        <!-- Actions -->
                        <div class="flex flex-wrap justify-end gap-2">
                            <button onclick='openEditSeatModal({{ seat.id }}, `{{ seat.title }}`, `{{ seat.group_level }}`, `{{ seat.qualifier }}`, `{{ seat.duty|default("committee_member") }}`)' class="inline-flex items-center px-2.5 py-1.5 bg-white border border-slate-200 text-xs font-bold text-slate-600 rounded hover:bg-indigo-50 hover:text-indigo-700 hover:border-indigo-200 shadow-sm transition">
                                <i class="fas fa-pencil-alt mr-1.5 opacity-70"></i> Edit
                            </button>
                            
                            <button onclick='openAssignModal(`{{ seat.title }}`)' class="inline-flex items-center px-2.5 py-1.5 bg-white border border-slate-200 text-xs font-bold text-slate-600 rounded hover:bg-emerald-50 hover:text-emerald-700 hover:border-emerald-200 shadow-sm transition">
                                <i class="fas fa-user-plus mr-1.5 opacity-70"></i> Assign
                            </button>

                            {% if seat.member %}
                            <button onclick='openPhotoModal({{ seat.member.id }})' class="inline-flex items-center px-2.5 py-1.5 bg-white border border-slate-200 text-xs font-bold text-slate-600 rounded hover:bg-amber-50 hover:text-amber-700 hover:border-amber-200 shadow-sm transition">
                                <i class="fas fa-camera mr-1.5 opacity-70"></i> Photo
                            </button>
                            {% endif %}
                        </div>
                    </td>"""

text = text.replace(old_actions, new_actions)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated Actions column to use text labels and wrap")
