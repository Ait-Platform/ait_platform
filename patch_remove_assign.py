with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# 1. Remove Assign Button
# We need to clean up the entire Action Buttons block to remove Assign entirely.
# Let's find the actions block and replace it.
pattern_actions = r'<!-- Actions -->.*?</div>\s*</td>'
new_actions = """<!-- Actions -->
                        <div class="flex items-center justify-end space-x-2">
                            <button onclick='openEditSeatModal({{ seat.id }}, `{{ seat.title }}`, `{{ seat.group_level }}`, `{{ seat.qualifier }}`, `{{ seat.duty|default("committee_member") }}`)' class="inline-flex items-center px-2 py-1 rounded bg-rose-50 border border-rose-200 text-rose-700 hover:text-rose-800 hover:border-rose-300 hover:bg-rose-100 shadow-sm transition text-xs font-bold" title="Edit Seat Duty">
                                <i class="fas fa-pencil-alt mr-1.5 opacity-50"></i> Edit
                            </button>
                            
                            {% if seat.member %}
                            <button onclick='openPhotoModal({{ seat.member.id }})' class="inline-flex items-center px-2 py-1 rounded bg-amber-50 border border-amber-200 text-amber-700 hover:text-amber-800 hover:border-amber-300 hover:bg-amber-100 shadow-sm transition text-xs font-bold" title="Upload Photo">
                                <i class="fas fa-camera mr-1.5 opacity-50"></i> Photo
                            </button>
                            {% else %}
                            <div class="inline-flex items-center px-2 py-1 rounded bg-slate-50 border border-slate-100 text-slate-400 cursor-not-allowed text-xs font-bold" title="Waiting for onboarding claim">
                                <i class="fas fa-user-clock mr-1.5 opacity-50"></i> Pending
                            </div>
                            {% endif %}
                        </div>
                    </td>"""

text = re.sub(pattern_actions, new_actions, text, flags=re.DOTALL)

# 2. Remove the assignMemberModal
pattern_modal = r'<!-- Assign Member Modal -->.*?<!-- Add Seat Modal -->'
text = re.sub(pattern_modal, '<!-- Add Seat Modal -->', text, flags=re.DOTALL)

# 3. Remove openAssignModal JS
pattern_js = r'function openAssignModal\(seatTitle\).*?\}'
text = re.sub(pattern_js, '', text, flags=re.DOTALL)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)

# 4. Remove backend logic from secretary_routes.py
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    routes = f.read()

pattern_backend = r'elif action == "assign_member":.*?elif action == "upload_photo":'
routes = re.sub(pattern_backend, 'elif action == "upload_photo":', routes, flags=re.DOTALL)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(routes)

print("Completely removed Assign button and logic")
