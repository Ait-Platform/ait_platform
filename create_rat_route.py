import os

# 1. Create the new ratification_desk route
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

route = """
@uip_bp.route("/<org_slug>/ratification-desk/<int:res_id>")
@login_required
def ratification_desk(org_slug, res_id):
    org = g.organization
    
    # Require EXCO or Committee member access
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not current_appointment:
        from flask import flash, abort
        abort(403)
        
    from app.models.uip import UipResolution
    resolution = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    if resolution.status != 'TABLED':
        from flask import flash
        flash("Only TABLED resolutions are at the Ratification Desk.", "info")
        return redirect(url_for('uip_bp.view_resolution', org_slug=org.slug, res_id=res_id))
        
    return render_template("program_uip/dashboards/ratification_desk.html", org=org, resolution=resolution, current_appointment=current_appointment)
"""

if "def ratification_desk" not in text:
    text += "\n" + route

# 2. Add redirect in view_resolution
redir = """    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    if res.status == 'TABLED':
        return redirect(url_for('uip_bp.ratification_desk', org_slug=org.slug, res_id=res.id))"""
        
text = text.replace("    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()", redir, 1)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added ratification_desk route")
