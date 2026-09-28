import re

filepath = 'app/program_uip/committee_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Change redirect at the end of decide_resolution
old_return = 'flash(f"Resolution officially {decision.lower()} and locked down.", "success")\\n    return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))'
new_return = 'flash("Mandate officially recorded and locked down.", "success")\\n    return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug, view="register"))'

content = content.replace(old_return, new_return)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("redirect updated")
