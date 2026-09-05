@uip_bp.route("/<org_slug>/reports")
@login_required
def org_reports(org_slug):
    from flask import g
    org = g.organization
    
    assignment = CoreRoleAssignment.query.filter_by(user_id=current_user.id, organization_id=org.id).first()
    if not assignment or assignment.role.slug not in ["manager", "committee_member"]:
        abort(403)
        
    return render_template("uip/dashboards/reports.html", org=org)

@uip_bp.route("/<org_slug>/reports/generate_ai", methods=["POST"])
@login_required
def generate_ai_report(org_slug):
    from flask import g
    from app.uip.gateway import LunaGateway
    org = g.organization
    
    # Compile raw data for Luna
    open_ix = CoreInteraction.query.filter(CoreInteraction.organization_id == org.id, CoreInteraction.status != 'RESOLVED').count()
    completed_wo = 0 # Dummy data for prototype
    
    prompt = f"Write a 3-sentence executive summary for the committee. Current metrics: {open_ix} open interactions, {completed_wo} completed work orders."
    
    result = LunaGateway.ask_luna(prompt)
    if result["status"] == "suspended":
        flash(result["message"], "warning")
    else:
        flash(f"Luna Management Summary: {result['message']} (Cost: {result['cost_cents']} AIT)", "success")
        
    return redirect(url_for("uip_bp.org_reports", org_slug=org_slug))
