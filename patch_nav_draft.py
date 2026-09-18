import re
with open("templates/program_uip/navigation.html", "r", encoding="utf-8") as f:
    text = f.read()

old_nav = """<details class="ui-nav-group" open>
    <summary>Secretary Tools</summary>
    <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}"><i class="fas fa-inbox w-5"></i> Intake Desk</a>
    <a href="#"><i class="fas fa-file-signature w-5"></i> Resolution Register</a>
    <a href="#"><i class="fas fa-users w-5"></i> Members Register</a>
    <a href="#"><i class="fas fa-user-tag w-5"></i> Role Assignment</a>
</details>"""

new_nav = """<details class="ui-nav-group" open>
    <summary>Secretary Tools</summary>
    <a href="{{ url_for('uip_bp.draft_resolution', org_slug=org.slug) }}" class="font-bold text-indigo-700 bg-indigo-50"><i class="fas fa-pen-nib w-5 text-indigo-600"></i> Draft Resolution</a>
    <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}"><i class="fas fa-inbox w-5"></i> Intake Desk</a>
    <a href="{{ url_for('uip_bp.committee_dashboard', org_slug=org.slug) }}"><i class="fas fa-file-signature w-5"></i> Resolution Register</a>
    <a href="#"><i class="fas fa-users w-5"></i> Members Register</a>
    <a href="#"><i class="fas fa-user-tag w-5"></i> Role Assignment</a>
</details>"""

text = text.replace(old_nav, new_nav)

with open("templates/program_uip/navigation.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated navigation.html with draft button")
