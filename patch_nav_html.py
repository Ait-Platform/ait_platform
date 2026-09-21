with open("templates/program_uip/navigation.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

pattern = r'{% if is_secretary %}.*?{% endif %}'

new_nav = """{% if is_chairman %}
<details class="ui-nav-group ui-nav-rose" open>
    <summary>Chairman Tools</summary>
    <a href="{{ url_for('uip_bp.chairman_voting_room', org_slug=org.slug) }}"><i class="fas fa-gavel w-5"></i> Voting & Mandates</a>
    <!-- Cloned modules to be built later -->
    <a href="#"><i class="fas fa-sitemap w-5"></i> Organogram (Coming Soon)</a>
    <a href="#"><i class="fas fa-coins w-5"></i> Financial Health (Coming Soon)</a>
    <a href="#"><i class="fas fa-hard-hat w-5"></i> Ground Ops (Coming Soon)</a>
</details>
{% elif is_secretary %}
<details class="ui-nav-group ui-nav-rose" open>
    <summary>Secretary Tools</summary>
    <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}"><i class="fas fa-toggle-on w-5"></i> Switchboard</a>
    <a href="{{ url_for('uip_bp.secretary_intake', org_slug=org.slug) }}"><i class="fas fa-inbox w-5"></i> Intake Desk</a>
    <a href="#"><i class="fas fa-users w-5"></i> Members Register</a>
    <a href="#"><i class="fas fa-user-tag w-5"></i> Role Assignment</a>
</details>
{% endif %}"""

text = re.sub(pattern, new_nav, text, flags=re.DOTALL)

with open("templates/program_uip/navigation.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated navigation.html")
