with open("templates/program_uip/dashboards/secretary_workspace.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace(
    """<!-- Tile 4: Organogram Builder -->
        <a href="#" class="group block relative overflow-hidden rounded-2xl border bg-purple-50 border-purple-100 hover:border-purple-300 hover:shadow-md transition-all duration-300">""",
    """<!-- Tile 4: Organogram Builder -->
        <a href="{{ url_for('uip_bp.secretary_organogram', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border bg-purple-50 border-purple-100 hover:border-purple-300 hover:shadow-md transition-all duration-300">"""
)

with open("templates/program_uip/dashboards/secretary_workspace.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated Switchboard link")
