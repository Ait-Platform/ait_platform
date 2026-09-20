with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

bad_string = """onclick="openEditSeatModal({{ seat.id }}, '{{ seat.title|replace(\"'\", \"\\'\") }}', '{{ seat.group_level }}', '{{ seat.qualifier }}', '{{ seat.duty|default('committee_member') }}')\""""

good_string = """onclick='openEditSeatModal({{ seat.id }}, `{{ seat.title }}`, `{{ seat.group_level }}`, `{{ seat.qualifier }}`, `{{ seat.duty|default("committee_member") }}`)'"""

text = text.replace(bad_string, good_string)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Replaced bad string directly")
