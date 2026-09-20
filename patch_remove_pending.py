with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

old_actions = """                            {% if seat.member %}
                            <button onclick='openPhotoModal({{ seat.member.id }})' class="inline-flex items-center px-2 py-1 rounded bg-amber-50 border border-amber-200 text-amber-700 hover:text-amber-800 hover:border-amber-300 hover:bg-amber-100 shadow-sm transition text-xs font-bold" title="Upload Photo">
                                <i class="fas fa-camera mr-1.5 opacity-50"></i> Photo
                            </button>
                            {% else %}
                            <div class="inline-flex items-center px-2 py-1 rounded bg-slate-50 border border-slate-100 text-slate-400 cursor-not-allowed text-xs font-bold" title="Waiting for onboarding claim">
                                <i class="fas fa-user-clock mr-1.5 opacity-50"></i> Pending
                            </div>
                            {% endif %}"""

new_actions = """                            {% if seat.member %}
                            <button onclick='openPhotoModal({{ seat.member.id }})' class="inline-flex items-center px-2 py-1 rounded bg-amber-50 border border-amber-200 text-amber-700 hover:text-amber-800 hover:border-amber-300 hover:bg-amber-100 shadow-sm transition text-xs font-bold" title="Upload Photo">
                                <i class="fas fa-camera mr-1.5 opacity-50"></i> Photo
                            </button>
                            {% endif %}"""

text = text.replace(old_actions, new_actions)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Removed pending badge from actions column")
