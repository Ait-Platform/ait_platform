with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Add Level 3 HTML Block
level_3_html = """
    <!-- LEVEL 3: COMMUNITY & OPERATIONS -->
    <div class="mb-12">
        <h2 class="text-xl font-black text-slate-800 mb-6 flex items-center"><i class="fas fa-hard-hat text-amber-500 mr-2"></i> Community & Operations</h2>
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            {% for seat in operations_seats %}
            <div class="bg-white border {% if not seat.member %}border-rose-200 shadow-[0_0_15px_rgba(225,29,72,0.15)]{% else %}border-slate-200 shadow-sm{% endif %} rounded-2xl p-6 relative overflow-hidden transition-all hover:shadow-md">
                {% if not seat.member %}
                <div class="absolute top-0 right-0 bg-rose-500 text-white text-[9px] font-black tracking-widest px-3 py-1 rounded-bl-lg uppercase">Vacant</div>
                {% endif %}
                
                <div class="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">{{ seat.qualifier }}</div>
                <h3 class="font-bold text-slate-900 text-lg leading-tight mb-4">{{ seat.title }}</h3>
                
                {% if seat.member %}
                <div class="flex items-center space-x-4 mb-4">
                    <div class="w-16 h-16 rounded-full bg-slate-100 border-2 border-amber-100 overflow-hidden flex items-center justify-center shrink-0">
                        {% if seat.member.photo_url %}
                        <img src="{{ seat.member.photo_url }}" class="w-full h-full object-cover">
                        {% else %}
                        <i class="fas fa-user text-2xl text-slate-300"></i>
                        {% endif %}
                    </div>
                    <div>
                        <div class="font-bold text-slate-800 text-sm">{{ seat.member.name }}</div>
                        <div class="text-[11px] text-slate-500 truncate w-32" title="{{ seat.member.email }}">{{ seat.member.email }}</div>
                    </div>
                </div>
                <button onclick="openPhotoModal({{ seat.member.id }})" class="w-full py-2 bg-amber-50 hover:bg-amber-100 text-amber-700 text-xs font-bold rounded-lg border border-amber-200 transition">
                    <i class="fas fa-camera mr-1"></i> {% if seat.member.photo_url %}Update Photo{% else %}Upload Photo{% endif %}
                </button>
                {% else %}
                <div class="flex items-center space-x-4 mb-4 opacity-50 grayscale">
                    <div class="w-16 h-16 rounded-full bg-slate-50 border-2 border-slate-200 border-dashed flex items-center justify-center shrink-0">
                        <i class="fas fa-user-slash text-2xl text-slate-300"></i>
                    </div>
                    <div>
                        <div class="font-bold text-slate-400 text-sm">Unassigned</div>
                        <div class="text-[11px] text-slate-400">Waiting for assignment</div>
                    </div>
                </div>
                <button class="w-full py-2 bg-slate-50 text-slate-400 text-xs font-bold rounded-lg border border-slate-200 cursor-not-allowed">
                    <i class="fas fa-camera mr-1"></i> Upload Photo
                </button>
                {% endif %}
            </div>
            {% endfor %}
            
            {% if not operations_seats %}
            <div class="col-span-full py-12 text-center border-2 border-dashed border-slate-200 rounded-2xl bg-slate-50 text-slate-500">
                <i class="fas fa-hard-hat text-4xl mb-3 text-slate-300"></i>
                <p>No Operations or Community seats have been mapped to the blueprint yet.</p>
            </div>
            {% endif %}
        </div>
    </div>
"""

# Insert Level 3 before <!-- MODALS -->
text = text.replace("<!-- MODALS -->", level_3_html + "\n<!-- MODALS -->")

# 2. Add OPERATIONS to the dropdown in the Add Seat Modal
old_options = """                    <option value="CORE_EXCO">Core Executive Committee</option>
                    <option value="SECOND_GROUP" selected>Sub-Committees & General</option>"""
new_options = """                    <option value="CORE_EXCO">Core Executive Committee</option>
                    <option value="SECOND_GROUP" selected>Sub-Committees & General</option>
                    <option value="OPERATIONS">Community & Operations (Managers, Staff, Providers)</option>"""

text = text.replace(old_options, new_options)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated Organogram UI to include Operations")
