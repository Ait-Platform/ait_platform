import re
with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

old_badges = """                        {% elif res.status == 'PROPOSED' %}
                            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-amber-100 text-amber-800">Proposed</span>"""

new_badges = """                        {% elif res.status == 'PROPOSED' %}
                            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-indigo-100 text-indigo-800">Proposed</span>
                        {% elif res.status == 'DRAFT' %}
                            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-amber-100 text-amber-800">Draft</span>"""

text = text.replace(old_badges, new_badges)

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated badge colors")
