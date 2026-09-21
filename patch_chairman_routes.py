with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

new_routes = """
@uip_bp.route("/<org_slug>/chairman-voting-room")
@login_required
def chairman_voting_room(org_slug):
    org = g.organization
    
    # Fetch all resolutions for the dashboard
    from app.models.uip import UipResolution
    all_resolutions = UipResolution.query.filter_by(organization_id=org.id).order_by(UipResolution.created_at.desc()).all()
    
    return render_template(
        "program_uip/dashboards/chairman_voting_room.html",
        org=org,
        all_resolutions=all_resolutions
    )

@uip_bp.route("/<org_slug>/chairman-resolution/<int:res_id>")
@login_required
def chairman_view_resolution(org_slug, res_id):
    org = g.organization
    from app.models.uip import UipResolution
    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    # Fetch votes
    from app.models.uip_governance import UipResolutionVote
    votes = UipResolutionVote.query.filter_by(resolution_id=res.id).all()
    
    has_voted = any(v.user_id == current_user.id for v in votes)
    
    yea_count = len([v for v in votes if v.vote == 'YEA'])
    nay_count = len([v for v in votes if v.vote == 'NAY'])
    abstain_count = len([v for v in votes if v.vote == 'ABSTAIN'])
    
    return render_template(
        "program_uip/dashboards/chairman_resolution_view.html",
        org=org,
        resolution=res,
        votes=votes,
        has_voted=has_voted,
        yea_count=yea_count,
        nay_count=nay_count,
        abstain_count=abstain_count
    )

@uip_bp.route("/<org_slug>/chairman-resolution/<int:res_id>/vote", methods=["POST"])
@login_required
def chairman_vote_resolution(org_slug, res_id):
    org = g.organization
    from app.models.uip import UipResolution
    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    if res.status != "PROPOSED":
        flash("Voting is currently closed for this resolution.", "error")
        return redirect(url_for("uip_bp.chairman_view_resolution", org_slug=org.slug, res_id=res_id))
        
    vote_val = request.form.get("vote")
    if vote_val not in ["YEA", "NAY", "ABSTAIN"]:
        flash("Invalid vote selection.", "error")
        return redirect(url_for("uip_bp.chairman_view_resolution", org_slug=org.slug, res_id=res_id))
        
    from app.models.uip_governance import UipResolutionVote
    existing_vote = UipResolutionVote.query.filter_by(resolution_id=res.id, user_id=current_user.id).first()
    
    if existing_vote:
        existing_vote.vote = vote_val
        flash("Your vote has been updated.", "success")
    else:
        new_vote = UipResolutionVote(
            resolution_id=res.id,
            user_id=current_user.id,
            vote=vote_val
        )
        db.session.add(new_vote)
        flash("Your vote has been recorded successfully.", "success")
        
    db.session.commit()
    
    return redirect(url_for("uip_bp.chairman_view_resolution", org_slug=org.slug, res_id=res_id))
"""

text += new_routes

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Added Chairman routing logic")
