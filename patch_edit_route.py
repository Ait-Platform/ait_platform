import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_edit = """@uip_bp.route("/<org_slug>/resolution/<int:res_id>/edit", methods=["GET", "POST"])
@login_required
def edit_resolution(org_slug, res_id):
    org = g.organization
    _require_secretary()
    
    from app.models.uip import UipResolution
    from app import db
    
    resolution = UipResolution.query.filter_by(id=res_id, organization_id=org.id).first_or_404()
    if resolution.status not in ["PROPOSED", "DRAFT"]:
        flash("You cannot edit a resolution that has already been adopted or rejected.", "warning")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res_id))
        
    if request.method == "POST":
        resolution.title = request.form.get("title", resolution.title)
        resolution.description = request.form.get("description", resolution.description)
        db.session.commit()
        flash("Resolution text updated successfully.", "success")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res_id))
        
    return render_template("program_uip/dashboards/resolution_edit.html", org=org, resolution=resolution)"""

new_edit = """@uip_bp.route("/<org_slug>/resolution/<int:res_id>/edit", methods=["GET", "POST"])
@login_required
def edit_resolution(org_slug, res_id):
    org = g.organization
    
    exco_check = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not exco_check:
        flash("Only active Committee Members can edit drafts.", "error")
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
    
    resolution = UipResolution.query.filter_by(id=res_id, organization_id=org.id).first_or_404()
    if resolution.status != "DRAFT":
        flash("You can only edit resolutions while they are in DRAFT status.", "warning")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res_id))
        
    if request.method == "POST":
        resolution.title = request.form.get("title", resolution.title)
        resolution.description = request.form.get("description", resolution.description)
        resolution.voting_scope = request.form.get("voting_scope", resolution.voting_scope)
        resolution.quorum_target = request.form.get("quorum_target", type=int, default=resolution.quorum_target)
        db.session.commit()
        flash("Draft saved successfully.", "success")
        return redirect(url_for("uip_bp.edit_resolution", org_slug=org.slug, res_id=res_id))
        
    return render_template("program_uip/dashboards/resolution_draft.html", org=org, resolution=resolution)"""

text = text.replace(old_edit, new_edit)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated edit_resolution route")
