import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

new_routes = """
@uip_bp.route("/<org_slug>/resolution/draft", methods=["GET", "POST"])
@login_required
def draft_resolution(org_slug):
    org = g.organization
    _require_role("secretary") # Assuming secretary is required, but let's use the DB check
    
    # Check if Secretary
    sec_check = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        UipCommitteeMember.position == "Secretary",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not sec_check:
        flash("Only the Secretary can draft resolutions.", "error")
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))

    if request.method == "POST":
        title = request.form.get("title")
        description = request.form.get("description")
        voting_scope = request.form.get("voting_scope", "EXCO")
        quorum_target = request.form.get("quorum_target", type=int, default=50)

        # Ensure a meeting exists (even a placeholder for drafts)
        from app.models.uip import UipCommitteeMeeting
        meeting = UipCommitteeMeeting.query.filter_by(organization_id=org.id, meeting_type="FOUNDING").first()
        # Fallback to any meeting if founding is gone, or just rely on the existing constraint
        meeting_id = meeting.id if meeting else None

        new_res = UipResolution(
            organization_id=org.id,
            meeting_id=meeting_id,
            title=title,
            description=description,
            status="DRAFT",
            voting_scope=voting_scope,
            quorum_target=quorum_target,
            recorded_by=current_user.id
        )
        db.session.add(new_res)
        db.session.commit()
        
        flash("Resolution draft saved successfully.", "success")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=new_res.id))

    return render_template("program_uip/dashboards/resolution_draft.html", org=org)

@uip_bp.route("/<org_slug>/resolution/<int:res_id>/publish", methods=["POST"])
@login_required
def publish_resolution(org_slug, res_id):
    org = g.organization
    
    sec_check = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        UipCommitteeMember.position == "Secretary",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not sec_check:
        flash("Only the Secretary can publish resolutions.", "error")
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
        
    resolution = UipResolution.query.filter_by(id=res_id, organization_id=org.id).first_or_404()
    if resolution.status != "DRAFT":
        flash("This resolution is already published.", "warning")
    else:
        resolution.status = "PROPOSED"
        db.session.commit()
        flash("Resolution published to the Digital Committee Room! ExCo members can now vote.", "success")
        
    return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=resolution.id))
"""

text = text + new_routes

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added draft routes to committee_routes.py")
