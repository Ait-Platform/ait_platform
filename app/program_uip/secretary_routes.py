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
    
    # 1. Allow supreme system owner
    from app.models.core import CoreRoleAssignment, CoreRole
    owner_assignment = CoreRoleAssignment.query.filter(
        CoreRoleAssignment.organization_id == org.id,
        CoreRoleAssignment.user_id == current_user.id,
        CoreRoleAssignment.role.has(CoreRole.slug == 'owner')
    ).first()
    
    if owner_assignment:
        return None
        
    # 2. Check committee position
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not current_appointment or not current_appointment.position:
        abort(403, description="Access restricted.")
        
    pos = current_appointment.position.strip().lower()
    allowed = ["secretary", "chairperson", "chairman", "vice-chairperson", "vice chairman", "treasurer", "manager"]
    
    if pos not in allowed:
        abort(403, description="Access restricted to the active Secretary and Chairperson.")
        
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
    
    
    # Calculate pending resolutions (PROPOSED) for the alert badge
    pending_resolutions = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").count()
    first_tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").first()
    
    switch_gate = 'red' if len(enriched_claims) > 0 else 'clear'
    if tabled_res > 0:
        switch_res = 'red'
    elif pending_resolutions > 0:
        switch_res = 'amber'
    else:
        switch_res = 'clear'
    
    return render_template(
        "program_uip/dashboards/secretary_workspace.html",
        org=org,
        open_claims=enriched_claims,
        proposed_resolutions=proposed_resolutions,
        pending_resolutions=pending_resolutions,
        switch_gate=switch_gate,
        switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=pending_resolutions,
        first_tabled_res=first_tabled_res
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
        
    if any(c.interaction_type == "staff_claim" for c in claims):
        from .services.operational_admission import require_secretary, permitted_roles
        require_secretary(org.id, current_user.id)
        if not all(c.interaction_type == "staff_claim" for c in claims):
            abort(400, description="Select Staff/Provider requests separately from governance claims.")
        return render_template("program_uip/dashboards/process_operational_claims.html", org=org,
            claims=claims, permitted_roles=permitted_roles)

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
    if target == "operational":
        from .services.operational_admission import admit
        admit(org.id, current_user.id, claim_ids, request.form)
        db.session.commit()
        flash("Staff/Provider access verified. Provider associations remain separate.", "success")
        return redirect(url_for("uip_bp.secretary_intake", org_slug=org.slug))
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
        
    if any(c.interaction_type == "staff_claim" for c in claims):
        abort(400, description="Use Secretary Staff/Provider verification for these requests.")

    from datetime import datetime
    current_year = datetime.now().year
    
    if target == "new" and all(c.interaction_type == "ratepayer_claim" for c in claims):
        target = "instant"

    if any(c.interaction_type == "mo_claim" for c in claims) and target == "instant":
        abort(403, description="MO authority requires the applicable Resolution.")
    if target in ["founding", "instant"]:
        founding_res = None
        if target == "founding":
            founding_res = UipResolution.query.filter_by(organization_id=org.id).filter(UipResolution.title.ilike("%Founding%")).first()
            
        if any(c.interaction_type == "mo_claim" for c in claims) and (not founding_res or founding_res.status != "ADOPTED"):
            abort(403, description="MO authority requires an adopted applicable Resolution.")
        if founding_res or target == "instant":
            additions = f"\n\n-- Added via Inaugural Roster (Term: {term_start} for {term_duration} months) --\n"
            for claim in claims:
                role_name = claim.interaction_type.replace('_claim', '').title()
                additions += f"- {claim.creator.name} ({claim.creator.email}) as {role_name}\n"
                claim.status = "VERIFIED"
                
                from app.models.core import CoreOrganizationMember, CoreRoleAssignment, CoreRole
                from app.models.uip import UipMemberProfile
                from sqlalchemy import func
                
                # 1. Ensure they have an active organization membership
                org_mem = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=claim.creator.id).first()
                
                if not org_mem:
                    # 2. Check the Vault (UipMemberProfile) to see if there is a Ghost Member to merge!
                    vault_record = UipMemberProfile.query.filter(
                        UipMemberProfile.organization_id == org.id,
                        func.lower(UipMemberProfile.email) == func.lower(claim.creator.email)
                    ).first()
                    
                    if vault_record and vault_record.membership_id:
                        ghost_mem = CoreOrganizationMember.query.get(vault_record.membership_id)
                        if ghost_mem and ghost_mem.user_id is None:
                            # MERGE: Attach the real user_id to the ghost row!
                            ghost_mem.user_id = claim.creator.id
                            ghost_mem.is_active = True
                            org_mem = ghost_mem
                            # Update Vault status
                            vault_record.eligibility_status = "eligible"
                    
                    # 3. If STILL no org_mem (not in vault), create a brand new one
                    if not org_mem:
                        org_mem = CoreOrganizationMember(organization_id=org.id, user_id=claim.creator.id, is_active=True)
                        db.session.add(org_mem)
                else:
                    org_mem.is_active = True
                    
                # Grant the appropriate role
                role_slug = "committee_member" if claim.interaction_type in ["committee_claim", "secretary_claim", "chairman_claim", "treasurer_claim"] else "municipal_officer" if claim.interaction_type == "mo_claim" else "owner"
                
                # Check for custom Duty from Organogram Seat
                if claim.interaction_type in ["committee_claim", "secretary_claim", "chairman_claim", "treasurer_claim"]:
                    pos = "Secretary" if claim.interaction_type == "secretary_claim" else (claim.title.split(": ")[-1] if ":" in claim.title else (claim.title.split(" - ")[-1] if " - " in claim.title else claim.title))
                    from app.models.uip_governance import UipOrganogramSeat
                    seat_record = UipOrganogramSeat.query.filter(UipOrganogramSeat.organization_id == org.id, func.lower(UipOrganogramSeat.title) == func.lower(pos.strip())).first()
                    if seat_record and seat_record.duty:
                        role_slug = seat_record.duty
                
                role_obj = CoreRole.query.filter_by(slug=role_slug).first()
                if role_obj:
                    # check if they have it
                    existing_role = CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id).first()
                    if not existing_role:
                        db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id))

                if claim.interaction_type in ['committee_claim', 'secretary_claim']:
                    from app.models.uip_governance import UipCommitteeMember
                    pos = "Secretary" if claim.interaction_type == "secretary_claim" else (claim.title.split(": ")[-1] if ":" in claim.title else (claim.title.split(" - ")[-1] if " - " in claim.title else "Unassigned"))
                    from app.models.uip_governance import UipCommitteeTerm
                    term = UipCommitteeTerm.query.filter_by(organization_id=org.id).first()
                    if not term:
                        term = UipCommitteeTerm(organization_id=org.id, term_name="Genesis Term")
                        db.session.add(term)
                        db.session.flush()
                        
                    from app.models.uip_governance import UipOrganogramSeat
                    from sqlalchemy import func
                    seat = UipOrganogramSeat.query.filter(
                        UipOrganogramSeat.organization_id == org.id,
                        func.lower(UipOrganogramSeat.title) == func.lower(pos)
                    ).first()
                    mem = UipCommitteeMember(
                        organization_id=org.id,
                        term_id=term.id,
                        user_id=claim.creator.id,
                          name=claim.creator.name,
                        email=claim.creator.email,
                        position=pos,
                        seat_id=seat.id if seat else None,
                        status="CURRENT"
                    )
                    db.session.add(mem)
                    
            if founding_res:
                founding_res.description += additions
            db.session.commit()
            if target == "instant":
                flash("Ratepayers successfully verified and granted access (no resolution required)!", "success")
                return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))
            else:
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
    audit.record(org.id, current_user.id, "decision.recorded", None)
    
    db.session.commit()
    flash(f"Successfully drafted Resolution {current_year}-{res_count}.", "success")
    return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))


@uip_bp.route("/<org_slug>/secretary/decline-claim/<int:claim_id>", methods=["POST"])
@login_required
def decline_claim(org_slug, claim_id):
    org = g.organization
    _require_secretary()
    
    from app.models.core import CoreInteraction
    from datetime import datetime, timezone
    
    claim = CoreInteraction.query.filter_by(
        id=claim_id,
        organization_id=org.id,
        status="OPEN"
    ).first_or_404()
    
    claim.status = "DECLINED"
    claim.closed_by = current_user.id
    claim.closed_at = datetime.now(timezone.utc)
    
    db.session.commit()
    flash("Access claim was removed/declined.", "success")
    return redirect(url_for("uip_bp.secretary_intake", org_slug=org_slug))

@uip_bp.route("/<org_slug>/secretary-intake")
@login_required
def secretary_intake(org_slug):
    org = g.organization
    _require_secretary()
    
    # Check for existing MO
    existing_mo = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.position.ilike('%Municipal%'),
        UipCommitteeMember.status == 'CURRENT'
    ).first()
    mo_conflict = False
    
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
        if claim.interaction_type == "mo_claim" and existing_mo:
            mo_conflict = True
        enriched_claims.append({
            "id": claim.id,
            "type": claim.interaction_type,
            "title": claim.title,
            "description": claim.description,
            "created_at": claim.created_at,
            "user_name": creator.name or "User",
            "user_email": creator.email,
            "creator_id": creator.id
        })
        
    from app.models.uip import UipResolution
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
    )

@uip_bp.route("/<org_slug>/verify-claim-mandate", methods=["POST"])
@login_required
def verify_claim_via_mandate(org_slug):
    from app.models.core import CoreOrganization, CoreInteraction, CoreRoleAssignment, CoreRole
    from app.models.uip import UipResolution
    from app.models.uip_governance import UipCommitteeMember, UipCommitteeTerm
    from sqlalchemy import func
    
    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()
    _require_secretary()
        
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
    if claim.interaction_type == "mo_claim":
        role_slug = "municipal_officer"
    elif claim.interaction_type == "committee_claim":
        role_slug = "committee_member"
    elif claim.interaction_type == "ratepayer_claim":
        role_slug = "ratepayer"
    else:
        role_slug = "subcommittee_member"
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
        position="Municipal Officer" if claim.interaction_type == "mo_claim" else ("Committee Member" if claim.interaction_type == "committee_claim" else "Ratepayer"),
        status="CURRENT"
    )
    db.session.add(new_member)
    
    # 4. Mark claim verified
    claim.status = "VERIFIED"
    claim.resolution_id = mandate.id
    
    db.session.commit()
    flash(f"{claim.creator.name} successfully verified against Mandate: {mandate.title}", "success")
    return redirect(url_for("uip_bp.secretary_intake", org_slug=org.slug))

@uip_bp.route("/<org_slug>/secretary/onboarding-campaign", methods=["GET", "POST"])
@login_required
def onboarding_campaign(org_slug):
    org = g.organization
    _require_secretary()
    
    from app.models.uip import UipMemberProfile, UipResolution, UipMemberCampaign
    from datetime import datetime, timedelta
    
    # Helper to get campaign state
    def get_campaign(profile):
        if not profile.campaign_status:
            c = UipMemberCampaign(member_profile_id=profile.id, invite_wave=0)
            db.session.add(c)
            db.session.flush()
            return c
        return profile.campaign_status

    if request.method == "POST":
        wave = request.form.get("wave")
        now = datetime.utcnow()
        unverified = UipMemberProfile.query.filter_by(organization_id=org.id, eligibility_status="unverified").all()
        count = 0
        
        for profile in unverified:
            camp = get_campaign(profile)
            if wave == "1" and camp.invite_wave == 0:
                camp.invite_wave = 1
                camp.last_invite_at = now
                count += 1
            elif wave == "2" and camp.invite_wave == 1 and camp.last_invite_at < (now - timedelta(days=3)):
                camp.invite_wave = 2
                camp.last_invite_at = now
                count += 1
            elif wave == "3" and camp.invite_wave == 2 and camp.last_invite_at < (now - timedelta(days=7)):
                camp.invite_wave = 3
                camp.last_invite_at = now
                count += 1
                
        flash(f"Wave {wave} dispatched to {count} ratepayers.", "success")
        db.session.commit()
        return redirect(url_for("uip_bp.onboarding_campaign", org_slug=org.slug))
        
    # GET Stats
    total_vault = UipMemberProfile.query.filter_by(organization_id=org.id).count() or 1
    active_members = UipMemberProfile.query.filter_by(organization_id=org.id, eligibility_status="eligible").count()
    activation_pct = int((active_members / total_vault) * 100)
    
    now = datetime.utcnow()
    unverified = UipMemberProfile.query.filter_by(organization_id=org.id, eligibility_status="unverified").all()
    
    wave_1_count = 0
    wave_2_count = 0
    wave_3_count = 0
    
    for profile in unverified:
        camp = profile.campaign_status
        if camp is None or camp.invite_wave == 0:
            wave_1_count += 1
        elif camp.invite_wave == 1 and camp.last_invite_at and camp.last_invite_at < (now - timedelta(days=3)):
            wave_2_count += 1
        elif camp.invite_wave == 2 and camp.last_invite_at and camp.last_invite_at < (now - timedelta(days=7)):
            wave_3_count += 1
            
    
    recent_resolutions = UipResolution.query.filter_by(organization_id=org.id, status="ADOPTED").order_by(UipResolution.created_at.desc()).limit(3).all()
    
    return render_template(
        "program_uip/dashboards/onboarding_campaign.html",
        org=org,
        quorum_target=50,
        activation_pct=activation_pct,
        active_members=active_members,
        wave_1_count=wave_1_count,
        wave_2_count=wave_2_count,
        wave_3_count=wave_3_count,
        recent_resolutions=recent_resolutions
    )

@uip_bp.route("/<org_slug>/secretary/organogram", methods=["GET", "POST"])
@login_required
def secretary_organogram(org_slug):
    org = g.organization
    _require_secretary()
    
    from app.models.uip_governance import UipOrganogramSeat, UipCommitteeMember
    
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add_seat":
            title = request.form.get("title")
            # Check for duplicate
            existing = UipOrganogramSeat.query.filter(
                UipOrganogramSeat.organization_id == org.id,
                db.func.lower(UipOrganogramSeat.title) == db.func.lower(title)
            ).first()
            if existing:
                flash(f"Warning: A seat with the title '{title}' already exists.", "danger")
            else:
                seat = UipOrganogramSeat(
                    organization_id=org.id,
                    title=title,
                    group_level=request.form.get("group_level"),
                    qualifier=request.form.get("qualifier"),
                    duty=request.form.get("duty", "committee_member"),
                    display_order=99
                )
                db.session.add(seat)
                db.session.commit()
                flash(f"Blueprint seat '{seat.title}' added.", "success")
        elif action == "edit_seat":
            seat_id = request.form.get("seat_id", type=int)
            seat = UipOrganogramSeat.query.get(seat_id)
            if seat and seat.organization_id == org.id:
                seat.title = request.form.get("title", seat.title)
                seat.group_level = request.form.get("group_level", seat.group_level)
                seat.qualifier = request.form.get("qualifier", seat.qualifier)
                seat.duty = request.form.get("duty", seat.duty)
                db.session.commit()
                flash(f"Blueprint seat '{seat.title}' updated.", "success")
        elif action == "record_subcommittee_membership":
            from .services.subcommittees import record_membership
            record_membership(org.id, current_user.id,
                request.form.get("subcommittee_id", type=int), request.form.get("member_id", type=int),
                request.form.get("resolution_id", type=int), request.form.get("valid_from"), request.form.get("valid_to"))
            db.session.commit()
            flash("Resolution-backed Subcommittee appointment recorded.", "success")
        elif action == "add_subcommittee":
            from app.program_uip.services import subcommittees as sub_service
            try:
                sub_service.create_subcommittee(
                    org.id, current_user.id,
                    request.form.get("name"),
                    request.form.get("resolution_id", type=int),
                    request.form.get("responsible_seat_id", type=int),
                    request.form.get("reports_to_seat_id", type=int)
                )
                db.session.commit()
                flash("Subcommittee registered successfully.", "success")
            except Exception as e:
                db.session.rollback()
                flash(str(e.description if hasattr(e, "description") else e), "danger")
        elif action == "upload_photo":
            member_id = request.form.get("member_id")
            photo_file = request.files.get("photo_file")
            member = UipCommitteeMember.query.get(member_id)
            if member and member.organization_id == org.id and photo_file:
                try:
                    from app.utils.cloudflare_r2 import upload_file_to_r2
                    photo_url = upload_file_to_r2(photo_file)
                    member.photo_url = photo_url
                    db.session.commit()
                    flash(f"Photo uploaded for {member.name} to Cloudflare R2.", "success")
                except Exception as e:
                    flash("Failed to upload photo. Please retry.", "danger")
        return redirect(url_for("uip_bp.secretary_organogram", org_slug=org.slug))
    
    # 1. Fetch Blueprint Seats
    core_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level="CORE_EXCO").order_by(UipOrganogramSeat.display_order).all()
    second_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level="SECOND_GROUP").order_by(UipOrganogramSeat.id).all()
    operations_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level="OPERATIONS").order_by(UipOrganogramSeat.id).all()
    
    # 2. Map active members to seats (For MVP: Map them by matching exact title since they don't have seat_ids properly linked in the DB from the genesis flow yet)
    active_members = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").all()
    
    # Attach members to seats temporarily for the view
    for seat in core_seats + second_seats + operations_seats:
        seat.member = None
        for m in active_members:
            if m.position.lower() == seat.title.lower():
                seat.member = m
                break
                
    from app.models.uip import UipResolution
    from app.program_uip.services import subcommittees as sub_service
    adopted_resolutions = UipResolution.query.filter_by(organization_id=org.id, status="ADOPTED").order_by(UipResolution.id.desc()).all()
    subcommittees = sub_service.get_subcommittees(org.id)
    for sub in subcommittees:
        sub.responsible_member = sub_service.resolve_responsible_member(sub)
                
    return render_template("program_uip/dashboards/secretary_organogram.html", org=org, core_seats=core_seats, second_seats=second_seats, operations_seats=operations_seats, active_members=active_members, adopted_resolutions=adopted_resolutions, subcommittees=subcommittees)
