with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Replace the disabled "Upload Photo" button with "Assign Member"
disabled_btn_html = """                <button class="w-full py-2 bg-slate-50 text-slate-400 text-xs font-bold rounded-lg border border-slate-200 cursor-not-allowed">
                    <i class="fas fa-camera mr-1"></i> Upload Photo
                </button>"""

assign_btn_html = """                <button onclick="openAssignModal('{{ seat.title }}')" class="w-full py-2 bg-slate-800 hover:bg-slate-900 text-white text-xs font-bold rounded-lg shadow-sm transition">
                    <i class="fas fa-user-plus mr-1"></i> Assign Member
                </button>"""

text = text.replace(disabled_btn_html, assign_btn_html)

# 2. Add assignMemberModal to the MODALS section
assign_modal = """
<!-- Assign Member Modal -->
<div id="assignMemberModal" class="hidden fixed inset-0 bg-slate-900/50 z-50 flex items-center justify-center backdrop-blur-sm">
    <div class="bg-white rounded-2xl shadow-xl max-w-md w-full overflow-hidden">
        <div class="p-6 border-b border-slate-100 flex justify-between items-center">
            <h3 class="font-bold text-lg text-slate-800">Assign Member</h3>
            <button type="button" onclick="document.getElementById('assignMemberModal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600"><i class="fas fa-times"></i></button>
        </div>
        <form method="POST" action="{{ url_for('uip_bp.secretary_organogram', org_slug=org.slug) }}" class="p-6 space-y-4">
            <input type="hidden" name="action" value="assign_member">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <input type="hidden" name="seat_title" id="assignSeatTitle">
            
            <p class="text-sm text-slate-600">Select an active, verified committee member to assign to the <strong id="assignSeatTitleDisplay" class="text-slate-900"></strong> seat.</p>

            <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Select Member</label>
                <select name="member_id" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                    <option value="" disabled selected>-- Select Member --</option>
                    {% for member in active_members %}
                    <option value="{{ member.id }}">{{ member.name }} ({{ member.email }})</option>
                    {% endfor %}
                </select>
            </div>
            
            <div class="pt-4 border-t border-slate-100 flex justify-end space-x-3">
                <button type="button" onclick="document.getElementById('assignMemberModal').classList.add('hidden')" class="px-4 py-2 bg-white border border-slate-200 text-slate-600 font-bold rounded-lg shadow-sm hover:bg-slate-50">Cancel</button>
                <button type="submit" class="px-4 py-2 bg-slate-800 hover:bg-slate-900 text-white font-bold rounded-lg shadow-sm transition">Assign Seat</button>
            </div>
        </form>
    </div>
</div>
"""
text = text.replace("<!-- MODALS -->", "<!-- MODALS -->" + assign_modal)

# 3. Add openAssignModal to JS
js_func = """function openAssignModal(seatTitle) {
    document.getElementById('assignSeatTitle').value = seatTitle;
    document.getElementById('assignSeatTitleDisplay').innerText = seatTitle;
    document.getElementById('assignMemberModal').classList.remove('hidden');
}

function toggleViewMode() {
    const builder = document.getElementById('builderMode');
    const presentation = document.getElementById('presentationMode');
    
    if (builder.classList.contains('hidden')) {
        builder.classList.remove('hidden');
        presentation.classList.add('hidden');
    } else {
        builder.classList.add('hidden');
        presentation.classList.remove('hidden');
    }
}"""
text = text.replace("function openPhotoModal", js_func + "\n\nfunction openPhotoModal")

# 4. Wrap Builder Mode and add Presentation Mode
# Find "<!-- ORGANOGRAM VISUALIZATION -->"
split_text = text.split("<!-- ORGANOGRAM VISUALIZATION -->")

presentation_mode = """
<div id="presentationMode" class="hidden mb-12">
    <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-8">
        <h2 class="text-2xl font-black text-center text-slate-900 mb-8">{{ org.name }} Organizational Blueprint</h2>
        
        <!-- Tier 1: Core ExCo -->
        <div class="mb-12">
            <h3 class="text-center text-sm font-bold uppercase tracking-widest text-indigo-500 mb-6">Core Executive</h3>
            <div class="flex flex-wrap justify-center gap-6">
                {% for seat in core_seats %}
                <div class="w-64 bg-indigo-50 border border-indigo-100 rounded-xl p-5 text-center shadow-sm">
                    <div class="text-[10px] font-bold text-indigo-400 uppercase tracking-widest mb-2">{{ seat.qualifier }}</div>
                    <div class="font-bold text-slate-900 mb-3">{{ seat.title }}</div>
                    {% if seat.member %}
                    <div class="text-sm font-bold text-indigo-700">{{ seat.member.name }}</div>
                    {% else %}
                    <div class="text-sm font-bold text-rose-500 animate-pulse">VACANT</div>
                    {% endif %}
                </div>
                {% endfor %}
            </div>
        </div>

        <!-- Tier 2: Sub-Committees -->
        <div class="mb-12 relative">
            <div class="absolute inset-0 flex items-center justify-center -z-10" style="top: -40px;">
                <div class="h-12 w-0.5 bg-slate-200"></div>
            </div>
            <h3 class="text-center text-sm font-bold uppercase tracking-widest text-emerald-500 mb-6">Sub-Committees</h3>
            <div class="flex flex-wrap justify-center gap-6">
                {% for seat in second_seats %}
                <div class="w-64 bg-emerald-50 border border-emerald-100 rounded-xl p-5 text-center shadow-sm">
                    <div class="text-[10px] font-bold text-emerald-400 uppercase tracking-widest mb-2">{{ seat.qualifier }}</div>
                    <div class="font-bold text-slate-900 mb-3">{{ seat.title }}</div>
                    {% if seat.member %}
                    <div class="text-sm font-bold text-emerald-700">{{ seat.member.name }}</div>
                    {% else %}
                    <div class="text-sm font-bold text-rose-500">TBA</div>
                    {% endif %}
                </div>
                {% endfor %}
            </div>
        </div>

        <!-- Tier 3: Operations -->
        <div class="relative">
            <div class="absolute inset-0 flex items-center justify-center -z-10" style="top: -40px;">
                <div class="h-12 w-0.5 bg-slate-200"></div>
            </div>
            <h3 class="text-center text-sm font-bold uppercase tracking-widest text-amber-500 mb-6">Operations & Execution</h3>
            <div class="flex flex-wrap justify-center gap-6">
                {% for seat in operations_seats %}
                <div class="w-64 bg-amber-50 border border-amber-100 rounded-xl p-5 text-center shadow-sm">
                    <div class="text-[10px] font-bold text-amber-400 uppercase tracking-widest mb-2">{{ seat.qualifier }}</div>
                    <div class="font-bold text-slate-900 mb-3">{{ seat.title }}</div>
                    {% if seat.member %}
                    <div class="text-sm font-bold text-amber-700">{{ seat.member.name }}</div>
                    {% else %}
                    <div class="text-sm font-bold text-rose-500">TBA</div>
                    {% endif %}
                </div>
                {% endfor %}
            </div>
        </div>
    </div>
</div>
"""

text = split_text[0] + "<!-- ORGANOGRAM VISUALIZATION -->\n" + presentation_mode + '\n<div id="builderMode">\n' + split_text[1]

# Close the div before <!-- MODALS -->
text = text.replace("<!-- MODALS -->", "</div>\n\n<!-- MODALS -->")

# Hook up the view button
text = text.replace('<button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-eye mr-2"></i> View</button>', '<button onclick="toggleViewMode()" class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-eye mr-2"></i> Toggle View</button>')

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
    
print("Patched frontend")
