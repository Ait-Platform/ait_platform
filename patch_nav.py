import re
with open("templates/program_uip/navigation.html", "r", encoding="utf-8") as f:
    text = f.read()

old_nav = """<details class="ui-nav-group" open>
    <summary>Secretary Tools</summary>
    <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}"><i class="fas fa-inbox w-5"></i> Intake Desk</a>
    <a href="#"><i class="fas fa-calendar-check w-5"></i> Meetings</a>
    <a href="#"><i class="fas fa-envelope-open-text w-5"></i> Correspondence</a>
    <a href="#"><i class="fas fa-book w-5"></i> Governance Record</a>
</details>"""

new_nav = """<details class="ui-nav-group" open>
    <summary>Secretary Tools</summary>
    <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}"><i class="fas fa-inbox w-5"></i> Intake Desk</a>
    <a href="#"><i class="fas fa-file-signature w-5"></i> Resolution Register</a>
    <a href="#"><i class="fas fa-users w-5"></i> Members Register</a>
    <a href="#"><i class="fas fa-user-tag w-5"></i> Role Assignment</a>
</details>"""

text = text.replace(old_nav, new_nav)

with open("templates/program_uip/navigation.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated navigation.html")
