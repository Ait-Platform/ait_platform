import re

# 1. Update about.html
with open('templates/program_sace/about.html', 'r', encoding='utf-8') as f:
    about_content = f.read()

about_content = about_content.replace(
    "url_for('auth_bp.register_decision', subject='sace_hub')",
    "url_for('sace_bp.catalog')"
)

with open('templates/program_sace/about.html', 'w', encoding='utf-8') as f:
    f.write(about_content)

# 2. Update sace_catalog.html
with open('templates/program_sace/sace_catalog.html', 'r', encoding='utf-8') as f:
    catalog_content = f.read()

# Replace the card link to point to registration
catalog_content = re.sub(
    r'href="\{\{ url_for\(\'sace_bp\.selection_hub\', activity_slug=activity\.slug\) \}\}"',
    r'href="{{ url_for(\'auth_bp.register_decision\', subject=\'sace_\' ~ activity.slug) }}"',
    catalog_content
)

with open('templates/program_sace/sace_catalog.html', 'w', encoding='utf-8') as f:
    f.write(catalog_content)

print("Updated links for Model B")
