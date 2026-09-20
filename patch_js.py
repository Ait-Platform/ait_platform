with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

import re
# Remove toggleViewMode function
text = re.sub(r'function toggleViewMode\(\) \{[\s\S]*?\}', '', text)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Removed toggleViewMode script")
