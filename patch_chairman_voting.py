with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# We will remove the left back button
text = re.sub(r'{% if current_appointment and current_appointment\.position == "Secretary" %}.*?{% endif %}', '', text, flags=re.DOTALL)

# Add back button to the TOP RIGHT (which is in row 1, but right aligned)
# Wait, let's just rewrite the header cleanly.
new_header = """<header class="ui-header-2row">
    <!-- Row 1: Title & Back Button -->
    <div class="ui-header-2row-top" style="display: flex; justify-content: space-between; align-items: center;">
        <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight ui-header-2row-title">Voting & Mandates</h1>
        <a href="{{ url_for('uip_bp.chairman_workspace', org_slug=org.slug) }}" class="inline-flex items-center px-4 py-2 bg-white border border-slate-300 text-sm font-bold text-slate-600 hover:text-indigo-600 hover:border-indigo-300 rounded-lg shadow-sm transition" style="white-space: nowrap;">
            Back to Chairman Dashboard <i class="fas fa-arrow-right ml-2"></i>
        </a>
    </div>
    <!-- Row 2: Subtitle & Action Buttons -->
    <div class="ui-header-2row-bottom mt-4">
        <p class="text-slate-500 font-medium ui-header-2row-subtitle">Official ledger of all drafted, proposed, and adopted resolutions.</p>
        <div class="ui-header-actions">
            <!-- No create resolution button for Chairman by default, but let's leave viewing tools -->
            <button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-eye mr-2"></i> View Log</button>
        </div>
    </div>
</header>"""

text = re.sub(r'<header class="ui-header-2row">.*?</header>', new_header, text, flags=re.DOTALL)

# Now, fix the links in the table to point to chairman_resolution_view
text = text.replace("url_for('uip_bp.view_resolution', org_slug=org.slug, res_id=res.id)", "url_for('uip_bp.chairman_view_resolution', org_slug=org.slug, res_id=res.id)")

with open("templates/program_uip/dashboards/chairman_voting_room.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Created chairman_voting_room.html")
