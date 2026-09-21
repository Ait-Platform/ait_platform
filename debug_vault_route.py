with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

route = """
@uip_bp.route("/<org_slug>/vault-check")
@login_required
def vault_check(org_slug):
    from app.models.uip import UipMemberProfile
    profiles = UipMemberProfile.query.filter_by(organization_id=g.organization.id).all()
    
    html = f"<h3>Vault List Check for {current_user.email}</h3>"
    html += "<p>Here are the emails currently in the Vault database:</p><ul>"
    
    found = False
    for p in profiles:
        match_str = " (MATCH!)" if p.email.lower().strip() == current_user.email.lower().strip() else ""
        if match_str: found = True
        html += f"<li>{p.name} - {p.email} - Active: {p.is_active}{match_str}</li>"
        
    html += "</ul>"
    if not found:
        html += f"<p style='color:red;'><b>Error:</b> {current_user.email} is NOT in the Vault!</p>"
    
    return html
"""

text += "\n" + route
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added vault diagnostic route")
