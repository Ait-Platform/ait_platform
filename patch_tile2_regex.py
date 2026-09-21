import re

for template in ["secretary_workspace.html", "chairman_workspace.html", "treasurer_workspace.html"]:
    path = f"templates/program_uip/dashboards/{template}"
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    
    # regex for the old text block
    pattern = r"\{\%\s*if\s*switch_res\s*==\s*'red'\s*\%\}.*?All\s*resolutions\s*up\s*to\s*date\..*?\{\%\s*endif\s*\%\}"
    
    new_text = """{% if switch_res == 'red' %}
                    {{ proposed_res }} Active | {{ tabled_res }} Tabled
                    {% elif switch_res == 'amber' %}
                    {{ proposed_res }} Active | 0 Tabled
                    {% else %}
                    All resolutions up to date.
                    {% endif %}"""
                    
    html = re.sub(pattern, new_text, html, flags=re.DOTALL)
    
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
print("Regex patch applied to workspaces")
