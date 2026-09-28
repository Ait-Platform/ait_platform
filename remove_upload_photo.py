import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Remove the upload_photo branch
new_dashboard = """def mo_dashboard(org_slug):
    org = g.organization
    _require_role("municipal_officer")
    
    from app.models.uip import UipMunicipalReferral, UipDocument
    from app.models.core import CoreInteraction
    escalations = UipMunicipalReferral.query.filter(
        UipMunicipalReferral.organization_id == org.id,
        UipMunicipalReferral.status.in_(["ESCALATED_TO_MO", "ACKNOWLEDGED", "DISPATCHED"])
    ).all()
    
    # Attach photos manually
    for ref in escalations:
        ref.photos = UipDocument.query.filter_by(
            organization_id=org.id,
            interaction_id=ref.interaction_id,
            category="RP_QUERY_PHOTO"
        ).all()
    
    return render_template("program_uip/dashboards/municipal_officer.html", org=org, escalations=escalations)"""

pattern = re.compile(r'def mo_dashboard\(org_slug\):.*?return render_template\("program_uip/dashboards/municipal_officer\.html", org=org, escalations=escalations\)', re.DOTALL)
text = pattern.sub(new_dashboard, text)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Removed upload_photo from routes")

with open("templates/program_uip/dashboards/municipal_officer.html", "r", encoding="utf-8") as f:
    html = f.read()

# Remove the upload_photo button and modal from the template
html = re.sub(r'<button onclick="document\.getElementById\(\'photoModal\'\)\.classList\.remove\(\'hidden\'\)".*?</button>', '', html, flags=re.DOTALL)
html = re.sub(r'<!-- Photo Upload Modal -->.*</div>\s*</div>\s*</div>', '', html, flags=re.DOTALL)

with open("templates/program_uip/dashboards/municipal_officer.html", "w", encoding="utf-8") as f:
    f.write(html)
print("Removed upload_photo from template")
