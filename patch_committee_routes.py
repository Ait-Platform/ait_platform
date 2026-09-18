import re

with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_view = """def view_resolution(org_slug, res_id):
    org = g.organization
    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    return render_template(
        "program_uip/dashboards/resolution_view.html",
        org=org,
        resolution=res,
        current_appointment=current_appointment
    )"""

new_view = """def view_resolution(org_slug, res_id):
    org = g.organization
    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    from app.models.uip_governance import UipCommitteeMember
    from app.models.uip import UipResolutionVote, UipResolutionComment
    from sqlalchemy import func
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    # Voting logic
    votes = res.votes.all() if hasattr(res, 'votes') else []
    comments = res.comments.all() if hasattr(res, 'comments') else []
    
    my_vote = next((v for v in votes if v.user_id == current_user.id), None)
    
    # Calculate Quorum
    quorum_target = getattr(res, 'quorum_target', 50)
    scope = getattr(res, 'voting_scope', 'EXCO')
    
    total_eligible = 0
    if scope == 'EXCO':
        total_eligible = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").count()
    else:
        from app.models.core import CoreOrganizationMember
        total_eligible = CoreOrganizationMember.query.filter_by(organization_id=org.id, is_active=True).count()
        
    if total_eligible == 0:
        total_eligible = 1 # Prevent division by zero
        
    current_quorum_pct = int((len(votes) / total_eligible) * 100)
    quorum_met = current_quorum_pct >= quorum_target
    
    # Vote counts
    yea_count = len([v for v in votes if v.vote == 'YEA'])
    nay_count = len([v for v in votes if v.vote == 'NAY'])
    abstain_count = len([v for v in votes if v.vote == 'ABSTAIN'])
    
    return render_template(
        "program_uip/dashboards/resolution_view.html",
        org=org,
        resolution=res,
        current_appointment=current_appointment,
        votes=votes,
        comments=comments,
        my_vote=my_vote,
        quorum_target=quorum_target,
        current_quorum_pct=current_quorum_pct,
        quorum_met=quorum_met,
        total_eligible=total_eligible,
        yea_count=yea_count,
        nay_count=nay_count,
        abstain_count=abstain_count,
        scope=scope
    )"""

text = text.replace(old_view, new_view)

# Add routes for voting and commenting
new_routes = """

@uip_bp.route("/<org_slug>/resolution/<int:res_id>/vote", methods=["POST"])
@login_required
def vote_resolution(org_slug, res_id):
    org = g.organization
    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    if res.status != "PROPOSED":
        flash("You can only vote on PROPOSED resolutions.", "danger")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
        
    from app.models.uip import UipResolutionVote
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    
    scope = getattr(res, 'voting_scope', 'EXCO')
    
    # Verify Eligibility
    if scope == 'EXCO':
        appointment = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org.id,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
        ).first()
        if not appointment:
            abort(403)
            
    vote_val = request.form.get("vote")
    if vote_val not in ["YEA", "NAY", "ABSTAIN"]:
        abort(400)
        
    existing = UipResolutionVote.query.filter_by(resolution_id=res.id, user_id=current_user.id).first()
    if existing:
        existing.vote = vote_val
        flash("Your vote has been updated.", "success")
    else:
        new_vote = UipResolutionVote(resolution_id=res.id, user_id=current_user.id, vote=vote_val)
        db.session.add(new_vote)
        flash("Your secure vote has been cast.", "success")
        
    db.session.commit()
    return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))


@uip_bp.route("/<org_slug>/resolution/<int:res_id>/comment", methods=["POST"])
@login_required
def comment_resolution(org_slug, res_id):
    org = g.organization
    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    message = request.form.get("message", "").strip()
    if not message:
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
        
    from app.models.uip import UipResolutionComment
    comment = UipResolutionComment(resolution_id=res.id, user_id=current_user.id, message=message)
    db.session.add(comment)
    db.session.commit()
    
    return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))

"""

if "def vote_resolution" not in text:
    text += new_routes

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated committee routes")
