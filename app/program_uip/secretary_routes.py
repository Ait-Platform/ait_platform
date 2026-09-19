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
    
    
    # Calculate pending resolutions (PROPOSED) for the alert badge
    pending_resolutions = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").count()
    
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
        proposed_res=pending_resolutions
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
    
    if target == "new" and all(c.interaction_type == "ratepayer_claim" for c in claims):
        target = "instant"

    if target in ["founding", "instant"]:
        founding_res = None
        if target == "founding":
            founding_res = UipResolution.query.filter_by(organization_id=org.id).filter(UipResolution.title.ilike("%Founding%")).first()
            
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
                role_slug = "committee_member" if claim.interaction_type in ["committee_claim", "secretary_claim", "chairman_claim", "treasurer_claim"] else "mo" if "mo" in claim.interaction_type else "resident"
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
                        
                    mem = UipCommitteeMember(
                        organization_id=org.id,
                        term_id=term.id,
                        name=claim.creator.name,
                        email=claim.creator.email,
                        position=pos,
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
        camp = get_campaign(profile)
        if camp.invite_wave == 0:
            wave_1_count += 1
        elif camp.invite_wave == 1 and camp.last_invite_at and camp.last_invite_at < (now - timedelta(days=3)):
            wave_2_count += 1
        elif camp.invite_wave == 2 and camp.last_invite_at and camp.last_invite_at < (now - timedelta(days=7)):
            wave_3_count += 1
            
    db.session.commit() # save any newly created campaign rows
    
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
    
    # Pre-populate basic 4 Core ExCo seats if Blueprint is totally empty
    if UipOrganogramSeat.query.filter_by(organization_id=org.id).count() == 0:
        default_seats = [
            ("Chairperson", "CORE_EXCO", "Voluntary", 1),
            ("Vice-Chairperson", "CORE_EXCO", "Voluntary", 2),
            ("Treasurer", "CORE_EXCO", "Voluntary", 3),
            ("Secretary", "CORE_EXCO", "Voluntary", 4),
            ("Security Sub-Committee Lead", "SECOND_GROUP", "Voluntary", 5),
            ("Greening & Environment Lead", "SECOND_GROUP", "Voluntary", 6),
            ("Infrastructure & Maintenance Lead", "SECOND_GROUP", "Voluntary", 7),
            ("Social & Community Lead", "SECOND_GROUP", "Voluntary", 8),
            ("Finance & Audit Lead", "SECOND_GROUP", "Voluntary", 9)
        ]
        for title, grp, qual, order in default_seats:
            seat = UipOrganogramSeat(organization_id=org.id, title=title, group_level=grp, qualifier=qual, display_order=order)
            db.session.add(seat)
        db.session.commit()
        
    if UipOrganogramSeat.query.filter_by(organization_id=org.id).count() == 4:
        new_seats = [
            ("Security Sub-Committee Lead", "SECOND_GROUP", "Voluntary", 5),
            ("Greening & Environment Lead", "SECOND_GROUP", "Voluntary", 6),
            ("Infrastructure & Maintenance Lead", "SECOND_GROUP", "Voluntary", 7),
            ("Social & Community Lead", "SECOND_GROUP", "Voluntary", 8),
            ("Finance & Audit Lead", "SECOND_GROUP", "Voluntary", 9)
        ]
        for title, grp, qual, order in new_seats:
            seat = UipOrganogramSeat(organization_id=org.id, title=title, group_level=grp, qualifier=qual, display_order=order)
            db.session.add(seat)
        db.session.commit()
    
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add_seat":
            seat = UipOrganogramSeat(
                organization_id=org.id,
                title=request.form.get("title"),
                group_level=request.form.get("group_level"),
                qualifier=request.form.get("qualifier"),
                display_order=99
            )
            db.session.add(seat)
            db.session.commit()
            flash(f"Blueprint seat '{seat.title}' added.", "success")
        elif action == "assign_member":
            seat_title = request.form.get("seat_title")
            member_id = request.form.get("member_id", type=int)
            if seat_title and member_id:
                from app.models.uip_governance import UipCommitteeMember
                member = UipCommitteeMember.query.filter_by(id=member_id, organization_id=org.id).first()
                if member:
                    member.position = seat_title
                    db.session.commit()
                    flash(f"{member.name} assigned to {seat_title}.", "success")
        elif action == "upload_photo":
            member_id = request.form.get("member_id")
            photo_url = request.form.get("photo_url")
            member = UipCommitteeMember.query.get(member_id)
            if member and member.organization_id == org.id:
                member.photo_url = photo_url
                db.session.commit()
                flash(f"Photo uploaded for {member.name}.", "success")
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
                
    return render_template("program_uip/dashboards/secretary_organogram.html", org=org, core_seats=core_seats, second_seats=second_seats, operations_seats=operations_seats, active_members=active_members)
