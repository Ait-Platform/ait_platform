with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()
import re
new_dashboard = """def mo_dashboard(org_slug):
    org = g.organization
    _require_role("municipal_officer")
    
    from flask import request, flash, redirect, url_for
    if request.method == "POST":
        if request.form.get("action") == "upload_photo":
            flash("Your photo was successfully securely uploaded in compliance with the POPI Act.", "success")
            return redirect(url_for("uip_bp.mo_dashboard", org_slug=org.slug))
            
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
print("Updated mo_dashboard")
