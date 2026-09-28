import re

filepath = 'app/program_uip/committee_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

route_code = """
@uip_bp.route("/<org_slug>/resolution/<int:res_id>/endorse", methods=["POST"])
@login_required
def endorse_ratification(org_slug):
    from app.models.core import CoreOrganization
    from app.models.uip import UipResolution
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    from datetime import datetime
    
    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()
    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    # Verify Lockdown Authority
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not current_appointment or current_appointment.position.lower() not in ["chairman", "chairperson", "chair", "vice chair", "vice chairman", "secretary", "treasurer"]:
        abort(403)
        
    basis = res.result_basis or {}
    rat = basis.get("ratification")
    if not rat:
        flash("No ratification record found to endorse.", "error")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
        
    if rat.get("recorded_by_email") == current_user.email:
        flash("You cannot endorse your own recording.", "error")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
        
    endorsements = rat.get("endorsements", [])
    if any(e.get("email") == current_user.email for e in endorsements):
        flash("You have already endorsed this record.", "info")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
        
    endorsements.append({
        "name": current_user.name,
        "email": current_user.email,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    })
    
    rat["endorsements"] = endorsements
    basis["ratification"] = rat
    
    # SQLAlchemy JSON mutation tracking requires reassignment
    from sqlalchemy.orm.attributes import flag_modified
    res.result_basis = basis
    flag_modified(res, "result_basis")
    
    db.session.commit()
    flash("You have successfully endorsed the ratification record.", "success")
    return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
"""

content += "\n" + route_code

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("endorse route added")
