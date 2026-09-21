with open("templates/program_uip/navigation.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# Find the is_exco block
pattern = r'{% if is_exco %}.*?{% endif %}'

new_block = """{% if is_exco %}
<a href="{{ url_for('uip_bp.committee_dashboard', org_slug=org.slug) }}" style="display: flex; align-items: center; padding: 0.75rem 1rem; font-weight: 700; color: #4338ca; background: #e0e7ff; border-radius: 0.5rem; margin-bottom: 1rem; text-decoration: none; transition: background 0.2s;" onmouseover="this.style.background='#c7d2fe'" onmouseout="this.style.background='#e0e7ff'">
    <i class="fas fa-th-large mr-3 text-indigo-600"></i> Secretary Control
</a>
{% endif %}"""

text = re.sub(pattern, new_block, text, flags=re.DOTALL)

with open("templates/program_uip/navigation.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated navigation.html to make Secretary Control a single clickable button")
