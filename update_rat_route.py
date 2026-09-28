import re

filepath = 'app/program_uip/committee_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the GET ratification_desk route with a simple redirect
old_route_pattern = re.compile(
    r'@uip_bp\.route\("/<org_slug>/ratification-desk/<int:res_id>"\)\s*@login_required\s*def ratification_desk\(org_slug, res_id\):.*?return render_template\("program_uip/dashboards/ratification_desk\.html", org=org, resolution=resolution, current_appointment=current_appointment\)',
    re.DOTALL
)

new_route = '''@uip_bp.route("/<org_slug>/ratification-desk/<int:res_id>")
@login_required
def ratification_desk(org_slug, res_id):
    # This old route is deprecated, redirecting to the new continuous desk
    return redirect(url_for('uip_bp.mandate_recording_desk', org_slug=org_slug))'''

content = old_route_pattern.sub(new_route, content)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Route updated")
