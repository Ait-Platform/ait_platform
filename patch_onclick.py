with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

import re
# Match: onclick="openEditSeatModal({{ seat.id }}, '{{ seat.title|replace("'", "\'") }}', '{{ seat.group_level }}', '{{ seat.qualifier }}', '{{ seat.duty|default('committee_member') }}')"
# We'll just replace the entire onclick string.

text = re.sub(r'onclick="openEditSeatModal\([^"]+"\)', 
              r'''onclick='openEditSeatModal({{ seat.id }}, `{{ seat.title }}`, `{{ seat.group_level }}`, `{{ seat.qualifier }}`, `{{ seat.duty|default("committee_member") }}`)\'''', 
              text)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Replaced onclick with backticks")
