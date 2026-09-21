with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    html = f.read()

# Change the header logic
old_header = "{% if resolution.status == 'ADOPTED' or resolution.status == 'REJECTED' %}Historical Resolution Record{% elif resolution.status == 'DRAFT' %}Drafting Desk{% else %}Digital Voting Room{% endif %}"
new_header = "{% if resolution.status == 'ADOPTED' or resolution.status == 'REJECTED' %}Historical Resolution Record{% elif resolution.status == 'DRAFT' %}Drafting Desk{% elif resolution.status == 'TABLED' %}Live Ratification Desk{% else %}Digital Voting Room{% endif %}"

html = html.replace(old_header, new_header)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(html)
print("Updated resolution view header")

# Now fix Tile 2 in the workspaces
for template in ["secretary_workspace.html", "chairman_workspace.html", "treasurer_workspace.html"]:
    path = f"templates/program_uip/dashboards/{template}"
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    
    old_text = """                    {% if switch_res == 'red' %}
                    {{ tabled_res }} TABLED - Needs Ratification.
                    {% elif switch_res == 'amber' %}
                    {{ proposed_res }} waiting for committee votes.
                    {% else %}
                    All resolutions up to date.
                    {% endif %}"""
                    
    new_text = """                    {% if switch_res == 'red' %}
                    {{ proposed_res }} Active | {{ tabled_res }} Tabled
                    {% elif switch_res == 'amber' %}
                    {{ proposed_res }} waiting for committee votes.
                    {% else %}
                    All resolutions up to date.
                    {% endif %}"""
                    
    html = html.replace(old_text, new_text)
    
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
print("Updated Tile 2 in workspaces")
