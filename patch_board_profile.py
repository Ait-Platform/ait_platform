import sys
with open("templates/program_uip/subcomm_tools/board.html", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    """<a href="{{ url_for('uip_bp.member_view', org_slug=org.slug, member_id=current_user.id) }}">Personal Profile</a>""",
    """<a href="{{ url_for('uip_bp.my_access', org_slug=org.slug) }}">Personal Profile</a>"""
)

with open("templates/program_uip/subcomm_tools/board.html", "w", encoding="utf-8") as f:
    f.write(c)
