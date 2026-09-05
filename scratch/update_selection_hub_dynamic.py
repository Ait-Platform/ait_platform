import re

with open('templates/program_sace/sace_selection_hub.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace static Title
content = content.replace("SACE Activity Hub", "Role Selection: {{ activity_slug|title }}")

# Fix the back button to go back to the catalog instead of bridge_dashboard
content = content.replace("url_for('auth_bp.bridge_dashboard')", "url_for('sace_bp.catalog')")

with open('templates/program_sace/sace_selection_hub.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated selection hub to be dynamic")
