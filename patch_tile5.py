import os
import re

# 1. Update committee_routes.py
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    c_text = f.read()

# Fix Chairman route
c_old_chair = """    # 3. Resolutions logic
    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    proposed_res = UipResolution.query.filter_by(organization_id=org.id, status="DRAFT").count()
"""
c_new_chair = """    # 3. Resolutions logic
    first_tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").first()
    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").count()
    proposed_res = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
"""
c_text = c_text.replace(c_old_chair, c_new_chair, 1)

# Fix Treasurer route
c_old_treas = """    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    proposed_res = UipResolution.query.filter_by(organization_id=org.id, status="DRAFT").count()
"""
c_new_treas = """    first_tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").first()
    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").count()
    proposed_res = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
"""
c_text = c_text.replace(c_old_treas, c_new_treas)

# Add first_tabled_res to chairman return
c_chair_ret_old = """        switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=proposed_res"""
c_chair_ret_new = """        switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=proposed_res,
        first_tabled_res=first_tabled_res"""
c_text = c_text.replace(c_chair_ret_old, c_chair_ret_new)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(c_text)


# 2. Update secretary_routes.py
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    s_text = f.read()

s_old = """tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").count()"""
s_new = """tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").count()
    first_tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").first()"""
s_text = s_text.replace(s_old, s_new)

s_ret_old = """switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=pending_resolutions"""
s_ret_new = """switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=pending_resolutions,
        first_tabled_res=first_tabled_res"""
s_text = s_text.replace(s_ret_old, s_ret_new)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(s_text)


# 3. Patch the HTML files for Tile 5
tile_replacement = """        <!-- Tile 5: Meetings & Agendas -->
        <a href="#" class="group block relative overflow-hidden rounded-2xl border {% if first_tabled_res %}bg-red-50 border-red-200 hover:border-red-300{% else %}bg-orange-50 border-orange-100 hover:border-orange-300{% endif %} hover:shadow-md transition-all duration-300">
            <div class="p-6">
                <div class="flex justify-between items-start mb-4">
                    <div class="w-12 h-12 rounded-full flex items-center justify-center text-xl {% if first_tabled_res %}bg-red-100 text-red-600{% else %}bg-orange-100 text-orange-500{% endif %}">
                        <i class="fas fa-calendar-check"></i>
                    </div>
                    {% if first_tabled_res %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-[10px] font-bold bg-red-600 text-white uppercase tracking-widest animate-pulse">
                        Urgent
                    </span>
                    {% else %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-[10px] font-bold bg-white text-orange-400 border border-orange-100 uppercase tracking-widest">
                        Schedule
                    </span>
                    {% endif %}
                </div>
                
                {% if first_tabled_res %}
                <h2 class="text-xl font-black text-red-900 mb-1">Live Meeting Required</h2>
                <div class="text-sm text-red-700 font-bold mb-2">Agenda: <span class="text-slate-800">{{ first_tabled_res.reference }} to be ratified</span></div>
                
                <div class="space-y-1 mt-3 pt-3 border-t border-red-100 text-xs font-bold text-red-800/80">
                    <div class="flex items-center"><i class="fas fa-calendar-day w-4 text-red-500"></i> Date: Pending Setup</div>
                    <div class="flex items-center"><i class="fas fa-clock w-4 text-red-500"></i> Time: Pending Setup</div>
                    <div class="flex items-center"><i class="fas fa-video w-4 text-red-500"></i> Format: Digital / Zoom (TBD)</div>
                </div>
                {% else %}
                <h2 class="text-xl font-black text-orange-900 mb-1">Meetings &amp; Agendas</h2>
                <p class="text-sm text-orange-600/70 font-medium">Schedule live meetings and capture minutes.</p>
                {% endif %}
            </div>
            <div class="h-1.5 w-full {% if first_tabled_res %}bg-red-500{% else %}bg-orange-400{% endif %} absolute bottom-0 left-0 opacity-0 group-hover:opacity-100 transition-opacity"></div>
        </a>"""

for template in ["secretary_workspace.html", "chairman_workspace.html", "treasurer_workspace.html"]:
    path = f"templates/program_uip/dashboards/{template}"
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    
    # Simple regex to replace the entire Tile 5 block
    # It starts with "<!-- Tile 5: Meetings & Agendas -->" and ends with "</a>"
    pattern = r'<!-- Tile 5: Meetings & Agendas -->.*?</a>'
    html = re.sub(pattern, tile_replacement, html, flags=re.DOTALL)
    
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

print("Updated Tile 5 on all dashboards")
