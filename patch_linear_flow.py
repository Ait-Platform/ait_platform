import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Remove 1-Nay escalation from vote_resolution
old_vote = """    # The 1-Nay Escalation Rule
    if vote_val == "NAY" and scope in ['EXCO_CORE', 'EXCO', 'COMMITTEE_ALL']:
        res.status = "ESCALATED"
        flash("You logged a dissenting vote. Digital voting has been automatically suspended and this resolution has been escalated to a live meeting.", "warning")
    else:
        flash("Your vote was securely recorded.", "success")"""

new_vote = """    flash("Your vote was securely recorded.", "success")"""
text = text.replace(old_vote, new_vote)

# 2. Add route to "Move to Live" (Table for meeting)
# Let's just create a new small block for it or modify decide_resolution
old_decide = """@uip_bp.route("/<org_slug>/resolution/<int:res_id>/decide", methods=["POST"])
@login_required
def decide_resolution(org_slug, res_id):
    org = g.organization
    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    # 1. Verify Lockdown Authority (Chairman or Vice Chair)
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not current_appointment or current_appointment.position.lower() not in ["chairman", "chairperson", "chair", "vice chair", "vice chairman", "secretary"]:
        flash("Only the Chairman, Vice Chairman, or Secretary has the authority to lock down and finalize resolutions.", "danger")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
        
    decision = request.form.get("decision")
    if decision not in ["ADOPTED", "REJECTED"]:
        abort(400)"""

new_decide = """@uip_bp.route("/<org_slug>/resolution/<int:res_id>/decide", methods=["POST"])
@login_required
def decide_resolution(org_slug, res_id):
    org = g.organization
    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    # 1. Verify Lockdown Authority (Chairman or Vice Chair or Secretary)
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not current_appointment or current_appointment.position.lower() not in ["chairman", "chairperson", "chair", "vice chair", "vice chairman", "secretary"]:
        flash("Only the Chairman, Vice Chairman, or Secretary has the authority to manage resolution stages.", "danger")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
        
    decision = request.form.get("decision")
    if decision not in ["ADOPTED", "REJECTED", "TABLED"]:
        abort(400)
        
    if decision == "TABLED":
        res.status = "TABLED"
        db.session.commit()
        flash("Voting closed. Resolution has been tabled for a live meeting.", "success")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))"""
text = text.replace(old_decide, new_decide)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated committee_routes.py for linear flow")
