import re
with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    content = f.read()

subcommittee_html = """

    <!-- Subcommittees Section -->
    <div class="mb-10">
        <div class="flex justify-between items-end border-b-2 border-slate-200 pb-3 mb-6">
            <div>
                <h3 class="text-xl font-bold text-slate-800">Formal Subcommittees</h3>
                <p class="text-sm text-slate-500">Established bodies delegated by resolution.</p>
            </div>
            <button onclick="document.getElementById('addSbcModal').classList.remove('hidden')" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-lg shadow-sm transition">
                <i class="fas fa-plus mr-2"></i> Register Subcommittee
            </button>
        </div>
        
        {% if subcommittees %}
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {% for sub in subcommittees %}
            <div class="bg-white border border-slate-200 rounded-2xl shadow-sm p-5 relative overflow-hidden group hover:shadow-md hover:border-indigo-200 transition-all">
                <div class="mb-3">
                    <span class="inline-block px-2.5 py-1 bg-green-50 text-green-700 font-bold text-[10px] uppercase tracking-wider rounded-md border border-green-200 mb-2">ACTIVE</span>
                    <h4 class="font-black text-slate-800 text-lg">{{ sub.name }}</h4>
                </div>
                
                <div class="space-y-3 mt-4 text-sm">
                    <div class="flex items-start">
                        <i class="fas fa-gavel text-slate-400 mt-1 mr-3"></i>
                        <div>
                            <span class="block text-xs font-bold text-slate-500 uppercase tracking-wide">Mandate</span>
                            <span class="text-slate-700">Res #{{ sub.establishing_resolution_id }}</span>
                        </div>
                    </div>
                    <div class="flex items-start">
                        <i class="fas fa-user-tie text-indigo-400 mt-1 mr-3"></i>
                        <div>
                            <span class="block text-xs font-bold text-slate-500 uppercase tracking-wide">Responsible Seat</span>
                            <span class="text-slate-700 font-medium">{% for s in second_seats %}{% if s.id == sub.responsible_seat_id %}{{ s.title }}{% endif %}{% endfor %}{% for s in core_seats %}{% if s.id == sub.responsible_seat_id %}{{ s.title }}{% endif %}{% endfor %}</span>
                            {% if sub.responsible_member %}
                            <span class="block text-xs text-indigo-600">{{ sub.responsible_member.name }}</span>
                            {% else %}
                            <span class="block text-xs text-red-500"><i class="fas fa-exclamation-triangle"></i> Vacant</span>
                            {% endif %}
                        </div>
                    </div>
                    <div class="flex items-start">
                        <i class="fas fa-level-up-alt text-slate-400 mt-1 mr-3"></i>
                        <div>
                            <span class="block text-xs font-bold text-slate-500 uppercase tracking-wide">Reports To</span>
                            <span class="text-slate-700">{% for s in core_seats %}{% if s.id == sub.reports_to_seat_id %}{{ s.title }}{% endif %}{% endfor %}{% for s in second_seats %}{% if s.id == sub.reports_to_seat_id %}{{ s.title }}{% endif %}{% endfor %}</span>
                        </div>
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>
        {% else %}
        <div class="text-center p-12 border-2 border-dashed border-slate-200 rounded-2xl">
            <div class="w-16 h-16 bg-slate-50 rounded-full flex items-center justify-center mx-auto mb-4 text-slate-400 text-2xl">
                <i class="fas fa-sitemap"></i>
            </div>
            <h4 class="font-bold text-slate-700 mb-1">No Subcommittees Registered</h4>
            <p class="text-sm text-slate-500">Register a formal subcommittee from an adopted resolution.</p>
        </div>
        {% endif %}
    </div>
"""

modal_html = """
  <!-- Add Subcommittee Modal -->
  <div id="addSbcModal" class="hidden fixed inset-0 bg-slate-900/50 z-50 flex items-center justify-center backdrop-blur-sm">
      <div class="bg-white rounded-2xl shadow-xl max-w-md w-full overflow-hidden">
          <div class="p-6 border-b border-slate-100 flex justify-between items-center">
              <h3 class="font-bold text-lg text-slate-800">Register Formal Subcommittee</h3>
              <button type="button" onclick="document.getElementById('addSbcModal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600"><i class="fas fa-times"></i></button>
          </div>
          <form method="POST" action="{{ url_for('uip_bp.secretary_organogram', org_slug=org.slug) }}" class="p-6 space-y-4">
              <input type="hidden" name="action" value="add_subcommittee">
              <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
              
              <div>
                  <label class="block text-xs font-bold text-slate-700 mb-1">Subcommittee Name</label>
                  <input type="text" name="name" required placeholder="e.g. Greening Subcommittee" class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
              </div>

              <div>
                  <label class="block text-xs font-bold text-slate-700 mb-1">Establishing Mandate (Adopted Resolution)</label>
                  <select name="resolution_id" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                      <option value="">Select adopted resolution...</option>
                      {% for res in adopted_resolutions %}
                      <option value="{{ res.id }}">Res #{{ res.id }} - {{ res.title }}</option>
                      {% endfor %}
                  </select>
              </div>
              
              <div>
                  <label class="block text-xs font-bold text-slate-700 mb-1">Responsible Organogram Seat</label>
                  <select name="responsible_seat_id" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                      <option value="">Select responsible elected office...</option>
                      <optgroup label="Core EXCO">
                      {% for s in core_seats %}
                      <option value="{{ s.id }}">{{ s.title }}</option>
                      {% endfor %}
                      </optgroup>
                      <optgroup label="Sub-Committees & General">
                      {% for s in second_seats %}
                      <option value="{{ s.id }}">{{ s.title }}</option>
                      {% endfor %}
                      </optgroup>
                  </select>
                  <p class="text-[10px] text-slate-500 mt-1">The current occupant of this seat will exercise responsibility.</p>
              </div>

              <div>
                  <label class="block text-xs font-bold text-slate-700 mb-1">Reports To Organogram Seat</label>
                  <select name="reports_to_seat_id" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                      <option value="">Select reporting line...</option>
                      <optgroup label="Core EXCO">
                      {% for s in core_seats %}
                      <option value="{{ s.id }}">{{ s.title }}</option>
                      {% endfor %}
                      </optgroup>
                      <optgroup label="Sub-Committees & General">
                      {% for s in second_seats %}
                      <option value="{{ s.id }}">{{ s.title }}</option>
                      {% endfor %}
                      </optgroup>
                  </select>
              </div>
              
              <div class="pt-4 border-t border-slate-100 flex justify-end space-x-3">
                  <button type="button" onclick="document.getElementById('addSbcModal').classList.add('hidden')" class="px-4 py-2 bg-white border border-slate-200 text-slate-600 font-bold rounded-lg shadow-sm hover:bg-slate-50">Cancel</button>
                  <button type="submit" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition">Register Subcommittee</button>
              </div>
          </form>
      </div>
  </div>
"""

content = content.replace("<!-- Add Seat Modal -->", subcommittee_html + "\n\n<!-- Add Seat Modal -->")
content = content.replace("<!-- Edit Seat Modal -->", modal_html + "\n\n<!-- Edit Seat Modal -->")

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(content)
print("Updated organogram UI")
