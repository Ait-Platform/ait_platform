from app.uip.gateway import LunaGateway

@uip_bp.route("/<org_slug>/interaction/<reference>/summarize", methods=["POST"])
@login_required
def summarize_interaction(org_slug, reference):
    from flask import g
    org = g.organization
    
    ix = CoreInteraction.query.filter_by(organization_id=org.id, reference=reference).first_or_404()
    
    # Check permissions
    assignment = CoreRoleAssignment.query.filter_by(user_id=current_user.id, organization_id=org.id).first()
    if not assignment or assignment.role.slug not in ["manager", "receptionist"]:
        abort(403)
        
    prompt = f"Please summarize this interaction briefly: {ix.description}"
    
    # Pass to AI Gateway
    result = LunaGateway.ask_luna(prompt, interaction_id=ix.id)
    
    if result["status"] == "suspended":
        flash(result["message"], "warning")
    else:
        # Add summary as a task or just flash it for now (simulating UI update)
        summary = result["message"]
        flash(f"Luna Summary: {summary} (Cost: {result['cost_cents']} credits, Remaining: {result['remaining_balance']})", "success")
        
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=reference))
