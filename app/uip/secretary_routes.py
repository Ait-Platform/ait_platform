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
    
    if not current_appointment or current_appointment.position != "Secretary":
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
            "ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim"
        ])
    ).order_by(CoreInteraction.created_at.asc()).all()
    
    # Enrich claims with user info
    enriched_claims = []
    for claim in open_claims:
        creator = User.query.get(claim.creator_id)
        enriched_claims.append({
            "id": claim.id,
            "type": claim.interaction_type,
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
        "uip/dashboards/secretary_workspace.html",
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
        
    # Mark as PENDING_RESOLUTION
    for claim in claims:
        claim.status = "PENDING_RESOLUTION"
        
    # Create the Resolution
    # We will use result_basis to store the interaction IDs that this resolution covers
    res = UipResolution(
        organization_id=org.id,
        title=f"Access Resolution ({len(claims)} Applicants)",
        description="Resolution to grant active platform access to the bundled applicants.",
        status="PROPOSED",
        recorded_by=current_user.id,
        result_basis={"type": "access_bundle", "interaction_ids": [c.id for c in claims]}
    )
    db.session.add(res)
    audit.record(org.id, current_user.id, "secretary.resolution_drafted", None)
    
    db.session.commit()
    flash(f"Successfully drafted resolution for {len(claims)} access claims.", "success")
    return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))
