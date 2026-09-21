import re

# 1. Update committee_routes.py for Treasurer routes
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    routes = f.read()

# A. Replace treasurer_workspace placeholder with logic
treasurer_placeholder = """@uip_bp.route("/<org_slug>/treasurer-workspace")
@login_required
def treasurer_workspace(org_slug):
    org = g.organization
    return render_template("program_uip/dashboards/placeholder_workspace.html", org=org, role_title="Treasurer", role_desc="Manage financial health, budgets, and procurement.")"""

treasurer_logic = """@uip_bp.route("/<org_slug>/treasurer-workspace")
@login_required
def treasurer_workspace(org_slug):
    org = g.organization
    from app.models.core import CoreInteraction
    from app.models.auth import User
    from app.models.uip import UipResolution
    
    # Fetch pending claims
    open_claims = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.status == "OPEN",
        CoreInteraction.interaction_type.in_([
            "committee_claim", "ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim", "unknown_claim"
        ])
    ).order_by(CoreInteraction.created_at.asc()).all()
    
    enriched_claims = []
    for claim in open_claims:
        creator = User.query.get(claim.creator_id)
        enriched_claims.append({
            "id": claim.id,
            "type": claim.interaction_type,
            "title": claim.title,
            "description": claim.description,
            "created_at": claim.created_at,
            "user_name": creator.name if creator else "Unknown",
            "user_email": creator.email if creator else "Unknown",
        })
        
    switch_gate = 'red' if enriched_claims else 'clear'
    
    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    proposed_res = UipResolution.query.filter_by(organization_id=org.id, status="DRAFT").count()
    
    if tabled_res > 0:
        switch_res = 'red'
    elif proposed_res > 0:
        switch_res = 'amber'
    else:
        switch_res = 'clear'
        
    return render_template(
        "program_uip/dashboards/treasurer_workspace.html",
        org=org,
        open_claims=enriched_claims,
        switch_gate=switch_gate,
        switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=proposed_res
    )

@uip_bp.route("/<org_slug>/treasurer-voting-room")
@login_required
def treasurer_voting_room(org_slug):
    org = g.organization
    from app.models.uip import UipResolution
    all_resolutions = UipResolution.query.filter_by(organization_id=org.id).order_by(UipResolution.created_at.desc()).all()
    return render_template("program_uip/dashboards/treasurer_voting_room.html", org=org, all_resolutions=all_resolutions)

@uip_bp.route("/<org_slug>/treasurer-resolution/<int:res_id>")
@login_required
def treasurer_view_resolution(org_slug, res_id):
    org = g.organization
    from app.models.uip import UipResolution
    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    from app.models.uip import UipResolutionVote
    votes = UipResolutionVote.query.filter_by(resolution_id=res.id).all()
    has_voted = any(v.user_id == current_user.id for v in votes)
    
    yea_count = len([v for v in votes if v.vote == 'YEA'])
    nay_count = len([v for v in votes if v.vote == 'NAY'])
    abstain_count = len([v for v in votes if v.vote == 'ABSTAIN'])
    
    return render_template(
        "program_uip/dashboards/treasurer_resolution_view.html",
        org=org,
        resolution=res,
        votes=votes,
        has_voted=has_voted,
        yea_count=yea_count,
        nay_count=nay_count,
        abstain_count=abstain_count
    )

@uip_bp.route("/<org_slug>/treasurer-resolution/<int:res_id>/vote", methods=["POST"])
@login_required
def treasurer_vote_resolution(org_slug, res_id):
    org = g.organization
    from app.models.uip import UipResolution
    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    if res.status != "PROPOSED":
        flash("Voting is currently closed for this resolution.", "error")
        return redirect(url_for("uip_bp.treasurer_view_resolution", org_slug=org.slug, res_id=res_id))
        
    vote_val = request.form.get("vote")
    if vote_val not in ["YEA", "NAY", "ABSTAIN"]:
        flash("Invalid vote selection.", "error")
        return redirect(url_for("uip_bp.treasurer_view_resolution", org_slug=org.slug, res_id=res_id))
        
    from app.models.uip import UipResolutionVote
    existing_vote = UipResolutionVote.query.filter_by(resolution_id=res.id, user_id=current_user.id).first()
    
    if existing_vote:
        existing_vote.vote = vote_val
        flash("Your vote has been updated.", "success")
    else:
        new_vote = UipResolutionVote(resolution_id=res.id, user_id=current_user.id, vote=vote_val)
        db.session.add(new_vote)
        flash("Your vote has been recorded successfully.", "success")
        
    db.session.commit()
    return redirect(url_for("uip_bp.treasurer_view_resolution", org_slug=org.slug, res_id=res_id))
"""

routes = routes.replace(treasurer_placeholder, treasurer_logic)
with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(routes)

print("Updated committee routes for treasurer")
