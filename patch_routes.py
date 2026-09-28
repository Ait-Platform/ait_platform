import re

filepath = 'app/program_uip/secretary_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Modify secretary_intake to pass adopted_resolutions
intake_pattern = re.compile(r'    return render_template\(\s*"program_uip/dashboards/secretary_intake\.html",\s*org=org,\s*open_claims=enriched_claims,\s*existing_mo=existing_mo,\s*mo_conflict=mo_conflict\s*\)')
intake_replacement = """    from app.models.uip import UipResolution
    adopted_resolutions = UipResolution.query.filter_by(
        organization_id=org.id, 
        status='ADOPTED'
    ).order_by(UipResolution.decision_date.desc().nullslast(), UipResolution.id.desc()).all()

    return render_template(
        "program_uip/dashboards/secretary_intake.html",
        org=org,
        open_claims=enriched_claims,
        existing_mo=existing_mo,
        mo_conflict=mo_conflict,
        adopted_resolutions=adopted_resolutions
    )"""
content = intake_pattern.sub(intake_replacement, content)

# 2. Replace record_mo_mandate with verify_claim_via_mandate
record_mo_pattern = re.compile(r'@uip_bp\.route\("/<org_slug>/record-mo-mandate", methods=\["POST"\]\).*?(?=\n\n@uip_bp\.route|\Z)', re.DOTALL)
verify_claim_logic = """@uip_bp.route("/<org_slug>/verify-claim-mandate", methods=["POST"])
@login_required
def verify_claim_via_mandate(org_slug):
    from app.models.core import CoreOrganization, CoreInteraction, CoreRoleAssignment, CoreRole
    from app.models.uip import UipResolution
    from app.models.uip_governance import UipCommitteeMember, UipCommitteeTerm
    from sqlalchemy import func
    
    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()
    if not _require_role("secretary", abort_on_fail=False) and not _require_role("manager", abort_on_fail=False):
        abort(403)
        
    claim_id = request.form.get("claim_id")
    mandate_id = request.form.get("mandate_id")
    action = request.form.get("action")
    
    claim = CoreInteraction.query.filter_by(organization_id=org.id, id=claim_id).first_or_404()
    
    if action == "reject":
        claim.status = "REJECTED"
        db.session.commit()
        flash("Claim rejected.", "info")
        return redirect(url_for("uip_bp.secretary_intake", org_slug=org.slug))
        
    mandate = UipResolution.query.filter_by(organization_id=org.id, id=mandate_id, status='ADOPTED').first_or_404()
    
    # 1. Check if there's an existing MO to replace if this is an MO claim
    if claim.interaction_type == "mo_claim":
        existing_mo = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org.id,
            UipCommitteeMember.position.ilike('%Municipal%'),
            UipCommitteeMember.status == 'CURRENT'
        ).first()
        if existing_mo:
            existing_mo.status = "FORMER"
            mo_role = CoreRole.query.filter_by(slug="municipal_officer").first()
            if mo_role:
                old_assignment = CoreRoleAssignment.query.filter_by(
                    organization_id=org.id,
                    user_id=existing_mo.user_id,
                    role_id=mo_role.id
                ).first()
                if old_assignment:
                    db.session.delete(old_assignment)
    
    # 2. Grant the system role
    role_slug = "municipal_officer" if claim.interaction_type == "mo_claim" else "subcommittee_member"
    role_record = CoreRole.query.filter_by(slug=role_slug).first()
    if role_record:
        assignment = CoreRoleAssignment(
            user_id=claim.creator_id,
            organization_id=org.id,
            role_id=role_record.id
        )
        db.session.add(assignment)
        
    # 3. Create Committee Member
    current_term = UipCommitteeTerm.query.filter_by(organization_id=org.id).order_by(UipCommitteeTerm.created_at.desc()).first()
    if not current_term:
        current_term = UipCommitteeTerm(organization_id=org.id, start_date=db.func.current_date())
        db.session.add(current_term)
        db.session.flush()
        
    new_member = UipCommitteeMember(
        organization_id=org.id,
        term_id=current_term.id,
        name=claim.creator.name,
        user_id=claim.creator_id,
        email=claim.creator.email,
        position="Municipal Officer" if claim.interaction_type == "mo_claim" else "Verified Member",
        status="CURRENT"
    )
    db.session.add(new_member)
    
    # 4. Mark claim verified
    claim.status = "VERIFIED"
    claim.resolution_id = mandate.id
    
    db.session.commit()
    flash(f"{claim.creator.name} successfully verified against Mandate: {mandate.title}", "success")
    return redirect(url_for("uip_bp.secretary_intake", org_slug=org.slug))"""
content = record_mo_pattern.sub(verify_claim_logic, content)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("secretary_routes updated")
