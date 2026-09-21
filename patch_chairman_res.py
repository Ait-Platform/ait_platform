with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# Remove the old back button on the left
text = re.sub(r'<a href="{{ url_for\(\'uip_bp.dashboard\', org_slug=org.slug\) }}".*?</a>', '', text, flags=re.DOTALL)

# Inject a new back button on the top right
# We find the flex container for the top row
top_row_pattern = r'(<div class="flex-grow pr-8">.*?</div>)'
# Actually, the layout has flex. Let's find the closing of the left column and insert the right button.
text = text.replace('{% if resolution.status == \'PROPOSED\' %}', """<!-- Top Right Back Button -->
<div class="absolute top-8 right-8">
    <a href="{{ url_for('uip_bp.chairman_voting_room', org_slug=org.slug) }}" class="inline-flex items-center px-4 py-2 bg-white border border-slate-300 text-sm font-bold text-slate-600 hover:text-indigo-600 hover:border-indigo-300 rounded-lg shadow-sm transition">
        Back to Voting Room <i class="fas fa-arrow-right ml-2"></i>
    </a>
</div>
{% if resolution.status == 'PROPOSED' %}""")

# Ensure the voting buttons render!
# Replace any EXCO_CORE check with a true condition for Chairman
text = text.replace("{% if current_appointment and current_appointment.group_level == 'EXCO_CORE' %}", "{% if True %}")

# Make sure the POST request goes to the normal vote endpoint but redirects correctly, 
# OR create a dedicated vote endpoint for chairman?
# The normal vote endpoint (`view_resolution` -> `POST /vote`) redirects back to `view_resolution`.
# We need to change the form action to point to `chairman_vote` so it redirects back to `chairman_resolution_view`.

text = text.replace("url_for('uip_bp.vote_resolution', org_slug=org.slug, res_id=resolution.id)", "url_for('uip_bp.chairman_vote_resolution', org_slug=org.slug, res_id=resolution.id)")


with open("templates/program_uip/dashboards/chairman_resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Created chairman_resolution_view.html")
