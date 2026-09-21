with open("templates/program_uip/navigation.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# Add treasurer block before secretary block
pattern = r'{% elif is_secretary %}'
replacement = """{% elif is_treasurer %}
<details class="ui-nav-group ui-nav-rose" open>
    <summary>Treasurer Tools</summary>
    <a href="{{ url_for('uip_bp.treasurer_voting_room', org_slug=org.slug) }}"><i class="fas fa-gavel w-5"></i> Voting & Mandates</a>
    <!-- Cloned modules to be built later -->
    <a href="#"><i class="fas fa-file-invoice-dollar w-5"></i> Financial Switchboard (Coming Soon)</a>
    <a href="#"><i class="fas fa-users w-5"></i> Ratepayer Registry (Coming Soon)</a>
</details>
{% elif is_secretary %}"""

text = text.replace(pattern, replacement)

with open("templates/program_uip/navigation.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated navigation HTML for treasurer")
