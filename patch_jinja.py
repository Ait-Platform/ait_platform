import re
with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

old_loop = """                        {% set has_pending = false %}
                        {% for res in all_resolutions %}
                            {% if res.status == 'PROPOSED' %}
                                {% set has_pending = true %}"""

new_loop = """                        {% set pending_list = all_resolutions | selectattr('status', 'equalto', 'PROPOSED') | list %}
                        {% for res in pending_list %}"""

text = text.replace(old_loop, new_loop)

old_end = """                        {% if not has_pending %}
                        <tr>"""

new_end = """                        {% if not pending_list %}
                        <tr>"""

text = text.replace(old_end, new_end)
# also remove the endif for the inner loop
text = text.replace("{% endif %}\n                        {% endfor %}", "{% endfor %}")

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated jinja loop")
