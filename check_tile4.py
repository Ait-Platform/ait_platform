with open("templates/program_uip/dashboards/secretary_workspace.html", "r", encoding="utf-8") as f:
    text = f.read()

import re
matches = re.search(r'<!-- Tile 4: Active Resolutions -->.*?</a>', text, re.DOTALL)
if matches:
    print(matches.group(0))
