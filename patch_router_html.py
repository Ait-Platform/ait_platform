with open("templates/program_uip/router.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# We will replace the left column starting with "Elected Members (ExCo)"
# and ending before "Right Column: Community & Operations"

pattern = re.compile(r'<h2 class="text-xl font-extrabold text-slate-800 mb-6 border-b border-slate-200 pb-2">Elected Members \(ExCo\).*?<!-- Right Column: Community & Operations -->', re.DOTALL)

dynamic_html = """<h2 class="text-xl font-extrabold text-slate-800 mb-6 border-b border-slate-200 pb-2">Elected Members (ExCo)</h2>
            <div class="space-y-4 mb-10">
                {% for seat in exco_seats %}
                {% set seat_lower = seat.title|lower %}
                {% set norm_seat = "chairperson" if seat_lower in ["chairman", "chair", "chairperson"] else ("vice-chairperson" if seat_lower in ["vice chairman", "vice chair", "vice-chairperson"] else seat_lower) %}
                {% set icon = 'fa-crown' if norm_seat == 'chairperson' else ('fa-coins' if norm_seat == 'treasurer' else ('fa-clipboard-check' if norm_seat == 'secretary' else 'fa-user-tie')) %}
                
                <form method="POST" action="{{ url_for('uip_bp.verify_committee', org_slug=org.slug) }}" class="block">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                    <input type="hidden" name="level" value="Executive Committee"/>
                    <input type="hidden" name="position" value="{{ seat.title }}"/>
                    {% if norm_seat in occupied_seats %}
                    <button disabled class="w-full p-4 text-left rounded-xl border border-slate-200 bg-slate-100 text-slate-400 font-bold text-lg cursor-not-allowed flex items-center justify-between opacity-75">
                        <span class="flex items-center"><i class="fas {{ icon }} text-2xl w-10 text-slate-400 mr-3 text-center"></i> {{ seat.title }}</span>
                        <span class="text-xs uppercase tracking-widest font-bold">Occupied</span>
                    </button>
                    {% else %}
                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas {{ icon }} text-2xl w-10 text-emerald-600 mr-3 text-center"></i> {{ seat.title }}</span>
                        <span class="text-xs uppercase tracking-widest font-bold text-emerald-600 border border-emerald-300 rounded px-2 py-1 bg-white">Vacant</span>
                    </button>
                    {% endif %}
                </form>
                {% endfor %}
            </div>
            
            <h2 class="text-xl font-extrabold text-slate-800 mb-6 border-b border-slate-200 pb-2">Subcommittees</h2>
            <div class="space-y-4 mb-10">
                {% for seat in sub_seats %}
                {% set seat_lower = seat.title|lower|trim %}
                <form method="POST" action="{{ url_for('uip_bp.verify_committee', org_slug=org.slug) }}" class="block">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                    <input type="hidden" name="level" value="Subcommittee"/>
                    <input type="hidden" name="position" value="{{ seat.title }}"/>
                    {% if seat_lower in occupied_seats %}
                    <button disabled class="w-full p-4 text-left rounded-xl border border-slate-200 bg-slate-100 text-slate-400 font-bold text-lg cursor-not-allowed flex items-center justify-between opacity-75">
                        <span class="flex items-center"><i class="fas fa-users-cog text-2xl w-10 text-slate-400 mr-3 text-center"></i> {{ seat.title }}</span>
                        <span class="text-xs uppercase tracking-widest font-bold">Occupied</span>
                    </button>
                    {% else %}
                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-purple-200 transition-transform hover:-translate-y-1 bg-purple-50 text-purple-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-users-cog text-2xl w-10 text-purple-600 mr-3 text-center"></i> {{ seat.title }}</span>
                        <span class="text-xs uppercase tracking-widest font-bold text-purple-600 border border-purple-300 rounded px-2 py-1 bg-white">Claim</span>
                    </button>
                    {% endif %}
                </form>
                {% endfor %}
            </div>
        </div>

        <!-- Right Column: Community & Operations -->"""

if pattern.search(text):
    text = pattern.sub(dynamic_html, text)
    with open("templates/program_uip/router.html", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched router.html with dynamic organogram seats!")
else:
    print("Regex failed to match!")
