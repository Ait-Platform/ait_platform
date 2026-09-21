with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

route = """
@uip_bp.route("/<org_slug>/public-mandates")
def public_mandates(org_slug):
    \"\"\"Public-facing read-only register of all officially adopted resolutions.\"\"\"
    from app.models.core import CoreOrganization
    from app.models.uip import UipResolution
    
    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()
    
    adopted_resolutions = UipResolution.query.filter_by(
        organization_id=org.id, 
        status='ADOPTED'
    ).order_by(
        UipResolution.decision_date.desc().nullslast(), 
        UipResolution.updated_at.desc()
    ).all()
    
    return render_template("program_uip/dashboards/public_mandates.html", org=org, resolutions=adopted_resolutions)
"""

if "def public_mandates" not in text:
    text += "\n" + route

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added public_mandates route")
