for template in ["secretary_workspace.html", "chairman_workspace.html", "treasurer_workspace.html"]:
    path = f"templates/program_uip/dashboards/{template}"
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    
    parts = html.split("mb-1\">Resolutions</h2>")
    if len(parts) > 1:
        # parts[1] contains:
        # <p class="text-sm ...">
        # {% if switch_res == 'red' %}
        # ...
        # </p>
        
        p_parts = parts[1].split("</p>", 1)
        
        new_p = """
                <p class="text-sm {% if switch_res == 'red' %}text-red-700 font-bold{% elif switch_res == 'amber' %}text-amber-700 font-bold{% else %}text-indigo-600/70 font-medium{% endif %}">
                    {% if switch_res == 'red' %}
                    {{ proposed_res }} Active | {{ tabled_res }} Tabled
                    {% elif switch_res == 'amber' %}
                    {{ proposed_res }} waiting for committee votes.
                    {% else %}
                    All resolutions up to date.
                    {% endif %}"""
                    
        html = parts[0] + "mb-1\">Resolutions</h2>" + new_p + "\n                </p>" + p_parts[1]
        
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
print("Manual patch applied for Tile 2 counts.")
