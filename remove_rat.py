with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    html = f.read()

import re

# Remove the Post-Meeting Ratification block
pattern = r'\{\%\s*if current_appointment and current_appointment\.position\.lower\(\) in \["secretary", "chairman", "vice chairman", "chair", "chairperson", "vice chair"\]\s*\%\}\s*<div class="mb-6 bg-slate-900 rounded-xl.*?\{\%\s*endif\s*\%\}'

new_html = re.sub(pattern, "", html, flags=re.DOTALL)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(new_html)
print("Removed ratification block from resolution_view")
