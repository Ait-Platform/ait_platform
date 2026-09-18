from flask import render_template, g, abort, redirect, url_for, request, flash
from flask_login import login_required, current_user

from app.extensions import db
from app.models.auth import User
from app.models.core import CoreInteraction
from app.models.uip import UipCommitteeMeeting, UipResolution
from app.models.uip_governance import UipCommitteeMember

from sqlalchemy import func

from . import uip_bp
from .services import audit

def _require_secretary():
    org = g.organization
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not current_appointment or not current_appointment.position or current_appointment.position.strip().lower() != "secretary":
        abort(403, description="Access restricted to the active Secretary.")
    return current_appointment

@uip_bp.route("/<org_slug>/secretary-workspace")
@login_required
def secretary_workspace(org_slug):
    org = g.organization
    _require_secretary()
    
    # 1. Fetch pending claims from strangers/users
    open_claims = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.status == "OPEN",
        CoreInteraction.interaction_type.in_([
            "committee_claim", "ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim", "unknown_claim"
        ])
    ).order_by(CoreInteraction.created_at.asc()).all()
    
    # Enrich claims with user info
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
        
    # 2. Fetch actively PROPOSED access resolutions
    proposed_resolutions = UipResolution.query.filter_by(
        organization_id=org.id,
        status="PROPOSED"
    ).filter(
        UipResolution.title.like("%Access Resolution%")
    ).order_by(UipResolution.created_at.desc()).all()
    
    return render_template(
        "program_uip/dashboards/secretary_workspace.html",
        org=org,
        open_claims=enriched_claims,
        proposed_resolutions=proposed_resolutions
    )

@uip_bp.route("/<org_slug>/draft-access-resolution", methods=["POST"])
@login_required
def draft_access_resolution(org_slug):
    org = g.organization
    _require_secretary()
    
    claim_ids = request.form.getlist("claim_ids[]")
    if not claim_ids:
        flash("No claims were selected to bundle.", "warning")
        return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))
        
    claims = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.id.in_(claim_ids),
        CoreInteraction.status == "OPEN"
    ).all()
    
    if not claims:
        flash("Selected claims are no longer open or valid.", "danger")
        return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))
        
    from datetime import datetime
    return render_template(
        "program_uip/dashboards/process_claims.html",
        org=org,
        claims=claims,
        datetime=datetime
    )

@uip_bp.route("/<org_slug>/finalize-access-resolution", methods=["POST"])
@login_required
def finalize_access_resolution(org_slug):
    org = g.organization
    _require_secretary()
    
    claim_ids = request.form.getlist("claim_ids[]")
    target = request.form.get("resolution_target", "new")
    term_start = request.form.get("term_start_date", "")
    term_duration = request.form.get("term_duration_months", "12")
    
    claims = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.id.in_(claim_ids),
        CoreInteraction.status == "OPEN"
    ).all()
    
    if not claims:
        flash("Selected claims are no longer open or valid.", "danger")
        return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))
        
    from datetime import datetime
    current_year = datetime.now().year
    
    if target == "founding":
        # Find the founding resolution
        founding_res = UipResolution.query.filter_by(organization_id=org.id).filter(UipResolution.title.ilike("%Founding%")).first()
        if founding_res:
            additions = f"\n\n-- Added via Inaugural Roster (Term: {term_start} for {term_duration} months) --\n"
            for claim in claims:
                role_name = claim.interaction_type.replace('_claim', '').title()
                additions += f"- {claim.creator.name} ({claim.creator.email}) as {role_name}\n"
                claim.status = "VERIFIED"
                
                from app.models.core import CoreOrganizationMember, CoreRoleAssignment, CoreRole
                
                # Ensure they have an active organization membership
                org_mem = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=claim.creator.id).first()
                if not org_mem:
                    org_mem = CoreOrganizationMember(organization_id=org.id, user_id=claim.creator.id, is_active=True)
                    db.session.add(org_mem)
                else:
                    org_mem.is_active = True
                    
                # Grant the appropriate role
                role_slug = "committee_member" if claim.interaction_type in ["committee_claim", "secretary_claim", "chairman_claim", "treasurer_claim"] else "mo" if "mo" in claim.interaction_type else "ratepayer"
                role_obj = CoreRole.query.filter_by(slug=role_slug).first()
                if role_obj:
                    # check if they have it
                    existing_role = CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id).first()
                    if not existing_role:
                        db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id))

                if claim.interaction_type in ['committee_claim', 'secretary_claim']:
                    from app.models.uip_governance import UipCommitteeMember
                    pos = "Secretary" if claim.interaction_type == "secretary_claim" else "Unassigned"
                    mem = UipCommitteeMember(
                        organization_id=org.id,
                        user_id=claim.creator.id,
                        name=claim.creator.name,
                        email=claim.creator.email,
                        position=pos,
                        status="CURRENT"
                    )
                    db.session.add(mem)
                    
            founding_res.description += additions
            db.session.commit()
            flash("Members successfully officially logged into the Founding Resolution!", "success")
            return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
            
    # Fallback or "new" resolution logic
    for claim in claims:
        claim.status = "PENDING_RESOLUTION"
        
    res_count = UipResolution.query.filter_by(organization_id=org.id).count() + 1
    
    res = UipResolution(
        organization_id=org.id,
        title=f"Resolution {current_year}-{res_count} - Access Bundle",
        description="Resolution to grant active platform access to the bundled applicants.\n",
        status="PROPOSED",
        recorded_by=current_user.id,
        result_basis={"type": "access_bundle", "interaction_ids": [c.id for c in claims]}
    )
    for claim in claims:
        role_name = claim.interaction_type.replace('_claim', '').title()
        res.description += f"\n- {claim.creator.name}: {role_name} (Term: {term_start} for {term_duration} months)"
        
    db.session.add(res)
    audit.record(org.id, current_user.id, "secretary.resolution_drafted", None)
    
    db.session.commit()
    flash(f"Successfully drafted Resolution {current_year}-{res_count}.", "success")
    return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))

@uip_bp.route("/<org_slug>/secretary-intake")
@login_required
def secretary_intake(org_slug):
    org = g.organization
    _require_secretary()
    
    # 1. Fetch pending claims from strangers/users
    open_claims = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.status == "OPEN",
        CoreInteraction.interaction_type.in_([
            "committee_claim", "ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim", "unknown_claim"
        ])
    ).order_by(CoreInteraction.created_at.asc()).all()
    
    # Enrich claims with user info
    enriched_claims = []
    for claim in open_claims:
        creator = User.query.get(claim.creator_id)
        enriched_claims.append({
            "id": claim.id,
            "type": claim.interaction_type,
            "title": claim.title,
            "description": claim.description,
            "created_at": claim.created_at,
            "user_name": creator.name or "User",
            "user_email": creator.email
        })
        
    return render_template(
        "program_uip/dashboards/secretary_intake.html",
        org=org,
        open_claims=enriched_claims
    )
