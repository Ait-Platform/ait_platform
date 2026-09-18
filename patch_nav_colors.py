import re
with open("templates/program_uip/navigation.html", "r", encoding="utf-8") as f:
    text = f.read()

# I will replace the <details class="ui-nav-group"> with specific colors
new_nav = """<nav aria-label="UIP Command Centre" class="ui-navigation">
{% if is_exco %}
<details class="ui-nav-group ui-nav-purple" open>
    <summary>Resolutions</summary>
    <a href="{{ url_for('uip_bp.committee_dashboard', org_slug=org.slug) }}"><i class="fas fa-file-signature w-5 text-indigo-600"></i> Resolution Register</a>
    <a href="{{ url_for('uip_bp.draft_resolution', org_slug=org.slug) }}"><i class="fas fa-pen-nib w-5 text-indigo-600"></i> Draft Resolution</a>
</details>
{% endif %}

{% if is_secretary %}
<details class="ui-nav-group ui-nav-rose" open>
    <summary>Secretary Tools</summary>
    <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}"><i class="fas fa-inbox w-5"></i> Intake Desk</a>
    <a href="#"><i class="fas fa-users w-5"></i> Members Register</a>
    <a href="#"><i class="fas fa-user-tag w-5"></i> Role Assignment</a>
</details>
{% endif %}

<details class="ui-nav-group ui-nav-teal" open>
    <summary>Personal</summary>
    <a href="{{ url_for('uip_bp.verify_ratepayer', org_slug=org.slug) }}"><i class="fas fa-home w-5"></i> Ratepayer Profile</a>
</details>

{% for group, items in uip_nav_groups.items() %}
<details class="ui-nav-group {{ loop.cycle('ui-nav-blue', 'ui-nav-amber', 'ui-nav-emerald', 'ui-nav-fuchsia', 'ui-nav-slate') }}" {% if items|selectattr('active')|list %}open{% endif %}>
    <summary>{{ group }}</summary>
    {% for item in items %}
    <a href="{{ item.href }}" {% if item.active %}aria-current="page"{% endif %}>{{ item.label }}</a>
    {% endfor %}
</details>
{% endfor %}
</nav>"""

# Using regex substitution since I know the exact structure
text = re.sub(r'<nav aria-label="UIP Command Centre" class="ui-navigation">.*?</nav>', new_nav, text, flags=re.DOTALL)

with open("templates/program_uip/navigation.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated navigation.html with color cycles")
