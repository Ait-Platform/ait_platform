
def execute_resolution_adoption(org, res, db):
    res.status = "ADOPTED"
    if res.result_basis and res.result_basis.get("type") == "access_bundle":
        from app.models.core import CoreInteraction, CoreOrganizationMember, CoreRoleAssignment, CoreRole
        from app.models.uip_governance import UipCommitteeMember
        
        interaction_ids = res.result_basis.get("interaction_ids", [])
        portfolio_map = res.result_basis.get("portfolios", {})
        
        claims = CoreInteraction.query.filter(CoreInteraction.id.in_(interaction_ids)).all()
        for claim in claims:
            claim.status = "VERIFIED"
            
            # Smart Merge: Look for a Ghost Row (Vault Upload without user_id)
            from app.models.uip import UipMemberProfile
            profile = UipMemberProfile.query.filter(UipMemberProfile.email.ilike(claim.creator.email)).first()
            if profile and profile.organization_member_id:
                org_mem = CoreOrganizationMember.query.get(profile.organization_member_id)
                if org_mem and not org_mem.user_id:
                    org_mem.user_id = claim.creator.id
                    org_mem.is_active = True
            
            # Ensure they have an active organization membership
            org_mem = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=claim.creator.id).first()
            if not org_mem:
                org_mem = CoreOrganizationMember(organization_id=org.id, user_id=claim.creator.id, is_active=True)
                db.session.add(org_mem)
            else:
                org_mem.is_active = True
                
            # Grant the role
            role_slug = "committee_member" if "committee" in claim.interaction_type or "secretary" in claim.interaction_type else "mo" if "mo" in claim.interaction_type else "ratepayer"
            role_obj = CoreRole.query.filter_by(slug=role_slug).first()
            if role_obj:
                existing_role = CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id).first()
                if not existing_role:
                    db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id))

            # If committee, make them an official member
            if 'committee' in claim.interaction_type or 'secretary' in claim.interaction_type:
                port = portfolio_map.get(str(claim.id)) or portfolio_map.get(claim.id) or ("Secretary" if "secretary" in claim.interaction_type else claim.interaction_type.replace('_claim', '').title())
                # Get or create a term
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
                    position=port,
                    status="CURRENT"
                )
                db.session.add(mem)
from flask import render_template, g, abort, redirect, url_for, request, flash, current_app
from flask_login import login_required, current_user
from itsdangerous import URLSafeTimedSerializer, BadSignature

from app.extensions import db
from app.models.auth import User
from app.models.core import CoreOrganizationMember, CoreRoleAssignment, CoreRole
from app.models.uip import UipCommitteeMeeting, UipResolution
from app.models.uip_governance import UipDelegation, UipCommitteeTerm, UipCommitteeMember

from sqlalchemy import func
from sqlalchemy.exc import ProgrammingError

from . import uip_bp
from .services import audit, governance
from .routes import _require_role

@uip_bp.route("/<org_slug>/committee-dashboard")
@login_required
def committee_dashboard(org_slug):
    org = g.organization
    
    # 1. Authorize: Must be current committee member or manager
    is_manager = _require_role("manager", abort_on_fail=False)
    
    try:
        current_appointment = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org.id,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
        ).first()
    except ProgrammingError:
        db.session.rollback()
        current_appointment = None

    if not current_appointment and not is_manager:
        abort(403)
        
    if current_appointment and current_appointment.position.lower() == "secretary":
        # Allow them to view the register if they explicitly clicked the tile
        if request.args.get("view") != "register":
            return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))
        
    current_term = None
    try:
        current_term = UipCommitteeTerm.query.filter_by(organization_id=org.id).order_by(UipCommitteeTerm.created_at.desc()).first()
    except ProgrammingError:
        db.session.rollback()
        
    committee_members = []
    if current_term:
        committee_members = UipCommitteeMember.query.filter_by(term_id=current_term.id).order_by(UipCommitteeMember.id).all()
        
    # Manager Resolution & Current Manager
    manager_resolution = UipResolution.query.join(UipCommitteeMeeting).filter(
        UipCommitteeMeeting.organization_id == org.id,
        UipResolution.title == "Manager Designation"
    ).order_by(UipResolution.created_at.desc()).first()
    
    current_manager = None
    if manager_resolution and manager_resolution.responsible_user_id:
        current_manager = User.query.get(manager_resolution.responsible_user_id)
            
    delegated_responsibility = None
    try:
        delegated_responsibility = UipDelegation.query.filter_by(
            organization_id=org.id,
            delegated_user_id=current_user.id,
            status="ACTIVE"
        ).first()
    except ProgrammingError:
        db.session.rollback()
    
    # Manager delegation controls
    ratepayer_admin = None
    ratepayer_admin_user = None
    if is_manager:
        try:
            ratepayer_admin = UipDelegation.query.filter_by(
                organization_id=org.id,
                delegation_type="RATEPAYER_ADMIN",
                status="ACTIVE"
            ).first()
            if ratepayer_admin:
                ratepayer_admin_user = User.query.get(ratepayer_admin.delegated_user_id)
        except ProgrammingError:
            db.session.rollback()
        
    from app.models.core import CoreInteraction
    
    upcoming_meetings_count = UipCommitteeMeeting.query.filter(
        UipCommitteeMeeting.organization_id == org.id,
        UipCommitteeMeeting.status != 'CONCLUDED'
    ).count()
    
    pending_resolutions_count = UipResolution.query.filter(
        UipResolution.organization_id == org.id,
        UipResolution.status.in_(['PROPOSED', 'DRAFT', 'PENDING'])
    ).count()
    
    committee_members_count = len(committee_members)
    
    new_matters_count = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.status == 'OPEN',
        CoreInteraction.interaction_type.in_(["ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim"])
    ).count()
    
    pending_claims = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.status == 'OPEN',
        CoreInteraction.interaction_type.in_(["ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim"])
    ).order_by(CoreInteraction.created_at.desc()).all()

    # Fetch all resolutions for the dashboard
    all_resolutions = UipResolution.query.filter_by(organization_id=org.id).order_by(UipResolution.created_at.desc()).all()
    
    return render_template(
        "program_uip/dashboards/committee.html",
        all_resolutions=all_resolutions,
        upcoming_meetings_count=upcoming_meetings_count,
        pending_resolutions_count=pending_resolutions_count,
        committee_members_count=committee_members_count,
        new_matters_count=new_matters_count,
        pending_claims=pending_claims,

        org=org,
        current_appointment=current_appointment,
        current_term=current_term,
        committee_members=committee_members,
        manager_resolution=manager_resolution,
        current_manager=current_manager,
        delegated_responsibility=delegated_responsibility,
        ratepayer_admin=ratepayer_admin,
        ratepayer_admin_user=ratepayer_admin_user,
        is_manager=is_manager
    )

@uip_bp.route("/<org_slug>/delegate/ratepayer-admin", methods=["POST"])
@login_required
def delegate_ratepayer_admin(org_slug):
    org = g.organization
    if not _require_role("manager", abort_on_fail=False):
        abort(403)
        
    delegated_email = request.form.get("delegated_email")
    if not delegated_email:
        delegated_email = request.form.get("delegated_user_id") # Backwards compatibility for frontend
        
    # Revoke existing
    try:
        existing = UipDelegation.query.filter_by(
            organization_id=org.id,
            delegation_type="RATEPAYER_ADMIN",
            status="ACTIVE"
        ).all()
        for d in existing:
            d.status = "REVOKED"
    except ProgrammingError:
        db.session.rollback()
        
    if not delegated_email:
        audit.record(org.id, current_user.id, "delegation.revoked", None)
        db.session.commit()
        flash("Delegation successfully revoked.", "info")
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
        
    # Find user by email
    delegated_user = User.query.filter(func.lower(User.email) == func.lower(delegated_email.strip())).first()
    if not delegated_user:
        abort(400, description="User not found.")
    
    # Must be an activated committee member via UipCommitteeMember
    try:
        is_committee = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org.id,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(delegated_user.email)
        ).first()
    except ProgrammingError:
        db.session.rollback()
        is_committee = None
    
    if not is_committee:
        abort(400, description="User is not an active committee member.")
        
    # Create new
    new_delegation = UipDelegation(
        organization_id=org.id,
        delegated_user_id=delegated_user.id,
        appointed_by_user_id=current_user.id,
        delegation_type="RATEPAYER_ADMIN",
        status="ACTIVE"
    )
    db.session.add(new_delegation)
    audit.record(org.id, current_user.id, "delegation.assigned", None)
    db.session.commit()
    
    flash("Ratepayer Register Administrator successfully delegated.", "success")
    return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))

@uip_bp.route("/<org_slug>/activate-committee/<token>", methods=["GET", "POST"])
def activate_committee(org_slug, token):
    from app.models.core import CoreOrganization
    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()
    
    signer = URLSafeTimedSerializer(current_app.secret_key, salt="uip-committee-activation")
    try:
        user_id = signer.loads(token, max_age=86400 * 7) # 7 days
    except BadSignature:
        abort(400, description="Activation link is invalid or expired.")
        
    user = User.query.get_or_404(user_id)
    if user.is_active:
        flash("Account is already active.", "info")
        return redirect(url_for("auth_bp.login"))
        
    if request.method == "POST":
        # Ensure they are currently an elected committee member
        try:
            is_elected = UipCommitteeMember.query.filter(
                UipCommitteeMember.organization_id == org.id,
                UipCommitteeMember.status == "CURRENT",
                func.lower(UipCommitteeMember.email) == func.lower(user.email)
            ).first()
        except ProgrammingError:
            db.session.rollback()
            is_elected = None
            
        if not is_elected:
            abort(403, description="Only current committee members can use this activation link.")
            
        roles_to_assign = []
        # Check if they are designated as manager
        man = UipResolution.query.join(UipCommitteeMeeting).filter(
            UipCommitteeMeeting.organization_id == org.id,
            UipResolution.title == "Manager Designation",
            UipResolution.responsible_user_id == user.id
        ).first()
        if man:
            roles_to_assign.append("manager")
            
        password = request.form.get("password")
        if not password or len(password) < 8:
            flash("Password must be at least 8 characters.", "danger")
            return redirect(request.url)
            
        user.set_password(password)
        user.is_active = 1
        
        membership = CoreOrganizationMember.query.filter_by(
            organization_id=org.id, user_id=user.id
        ).first()
        if membership:
            membership.is_active = True
                
        for role_slug in roles_to_assign:
            role = CoreRole.query.filter_by(slug=role_slug).first()
            if role:
                if not CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=user.id, role_id=role.id).first():
                    db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=user.id, role_id=role.id))
                    
        db.session.commit()
        flash("Committee account activated successfully. Please log in.", "success")
        return redirect(url_for("auth_bp.login"))
        
    return render_template("program_uip/activate_committee.html", org=org, user=user)

@uip_bp.route("/<org_slug>/committee/manage", methods=["GET", "POST"])
@login_required
def manage_committee(org_slug):
    org = g.organization
    # Only Managers or current committee members can manage terms
    is_manager = _require_role("manager", abort_on_fail=False)
    
    try:
        is_committee = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org.id,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
        ).first()
    except ProgrammingError:
        db.session.rollback()
        is_committee = None
        
    if not is_manager and not is_committee:
        abort(403)
        
    if request.method == "POST":
        term_name = request.form.get("term_name")
        emails = request.form.getlist("member_email[]")
        names = request.form.getlist("member_name[]")
        positions = request.form.getlist("member_position[]")
        
        if not term_name:
            flash("Term name is required.", "danger")
            return redirect(request.url)
            
        # Create new term
        new_term = UipCommitteeTerm(
            organization_id=org.id,
            term_name=term_name,
            created_by=current_user.id
        )
        db.session.add(new_term)
        db.session.flush()
        
        # Mark all old CURRENT members as FORMER
        old_members = UipCommitteeMember.query.filter_by(
            organization_id=org.id, status="CURRENT"
        ).all()
        for member in old_members:
            member.status = "FORMER"
            member.updated_by = current_user.id
            
        # Insert new members
        for email, name, position in zip(emails, names, positions):
            email = email.strip()
            name = name.strip()
            if not email or not name:
                continue
                
            new_member = UipCommitteeMember(
                term_id=new_term.id,
                organization_id=org.id,
                name=name,
                email=email,
                position=position,
                status="CURRENT",
                created_by=current_user.id
            )
            db.session.add(new_member)
            
        audit.record(org.id, current_user.id, "committee.term_created", None)
        db.session.commit()
        
        flash(f"New committee term '{term_name}' created successfully.", "success")
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
        
    return render_template("program_uip/manage_committee.html", org=org)

@uip_bp.route("/<org_slug>/resolution/<int:res_id>")
@login_required
def view_resolution(org_slug, res_id):
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
    if scope == 'EXCO_CORE':
        total_eligible = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org.id, 
            UipCommitteeMember.status == "CURRENT",
            UipCommitteeMember.position.in_(["Chairperson", "Vice-Chairperson", "Secretary", "Treasurer"])
        ).count()
    elif scope in ['EXCO', 'COMMITTEE_ALL', 'SUB_COMMITTEE']:
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
    )
@uip_bp.route("/<org_slug>/resolution/<int:res_id>/decide", methods=["POST"])
@login_required
def decide_resolution(org_slug, res_id):
    org = g.organization
    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    # 1. Verify Lockdown Authority (Chairman or Vice Chair or Secretary)
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not current_appointment or current_appointment.position.lower() not in ["chairman", "chairperson", "chair", "vice chair", "vice chairman", "secretary"]:
        flash("Only the Chairman, Vice Chairman, or Secretary has the authority to manage resolution stages.", "danger")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
        
    decision = request.form.get("decision")
    if decision not in ["ADOPTED", "REJECTED", "TABLED"]:
        abort(400)
        
    if decision == "TABLED":
        res.status = "TABLED"
        db.session.commit()
        flash("Voting closed. Resolution has been tabled for a live meeting.", "success")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
        
    # Enforce Quorum for Adoption
    if decision == "ADOPTED":
        votes = res.votes.all() if hasattr(res, 'votes') else []
        scope = getattr(res, 'voting_scope', 'EXCO')
        
        if scope in ['EXCO', 'EXCO_CORE', 'COMMITTEE_ALL', 'SUB_COMMITTEE']:
            # Get all eligible committee members
            if scope == 'EXCO_CORE':
                eligible_members = UipCommitteeMember.query.filter(
                    UipCommitteeMember.organization_id == org.id, 
                    UipCommitteeMember.status == "CURRENT",
                    UipCommitteeMember.position.in_(["Chairperson", "Vice-Chairperson", "Secretary", "Treasurer"])
                ).all()
            else:
                eligible_members = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").all()
            
            voted_user_ids = [v.user_id for v in votes]
            missing_members = [m.name for m in eligible_members if m.user_id not in voted_user_ids]
            
            if missing_members:
                missing_names = ", ".join(missing_members)
                flash(f"Resolution proceeded. WARNING: The following elected members violated the mandatory voting rule by failing to cast a digital vote: {missing_names}", "warning")
            else:
                flash("Resolution proceeded. All elected members successfully cast their mandatory digital votes.", "success")
                
        else:
            # For PUBLIC/Ratepayer scopes, we don't name-shame 400 people
            from app.models.core import CoreOrganizationMember
            total_eligible = CoreOrganizationMember.query.filter_by(organization_id=org.id, is_active=True).count()
            if total_eligible == 0: total_eligible = 1
            current_quorum_pct = int((len(votes) / total_eligible) * 100)
            flash(f"Public vote reached {current_quorum_pct}% participation. Proceeding to live ratification.", "info")
            
    res.status = decision
    
    # 2. If ADOPTED and it is an Access Bundle, grant the roles
    if decision == "ADOPTED" and res.result_basis and res.result_basis.get("type") == "access_bundle":
        from app.models.core import CoreInteraction, CoreOrganizationMember, CoreRoleAssignment, CoreRole
        
        interaction_ids = res.result_basis.get("interaction_ids", [])
        portfolio_map = res.result_basis.get("portfolios", {})
        
        claims = CoreInteraction.query.filter(CoreInteraction.id.in_(interaction_ids)).all()
        for claim in claims:
            claim.status = "VERIFIED"
            
            # Ensure they have an active organization membership
            org_mem = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=claim.creator.id).first()
            if not org_mem:
                org_mem = CoreOrganizationMember(organization_id=org.id, user_id=claim.creator.id, is_active=True)
                db.session.add(org_mem)
            else:
                org_mem.is_active = True
                
            # Grant the role
            role_slug = "committee_member" if "committee" in claim.interaction_type else "mo" if "mo" in claim.interaction_type else "ratepayer"
            role_obj = CoreRole.query.filter_by(slug=role_slug).first()
            if role_obj:
                existing_role = CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id).first()
                if not existing_role:
                    db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=claim.creator.id, role_id=role_obj.id))

            # If committee, make them an official member
            if 'committee' in claim.interaction_type:
                port = portfolio_map.get(str(claim.id)) or portfolio_map.get(claim.id) or claim.interaction_type.replace('_claim', '').title()
                mem = UipCommitteeMember(
                    organization_id=org.id,
                    user_id=claim.creator.id,
                    name=claim.creator.name,
                    email=claim.creator.email,
                    position=port,
                    status="CURRENT"
                )
                db.session.add(mem)
                
    db.session.commit()
    flash(f"Resolution officially {decision.lower()} and locked down.", "success")
    return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
@uip_bp.route("/<org_slug>/resolution/<int:res_id>/edit", methods=["GET", "POST"])
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
        
    return render_template("program_uip/dashboards/resolution_draft.html", org=org, resolution=resolution)


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
    appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if scope == 'EXCO_CORE':
        if not appointment or appointment.position not in ["Chairperson", "Vice-Chairperson", "Secretary", "Treasurer"]:
            abort(403)
    elif scope in ['EXCO', 'COMMITTEE_ALL', 'SUB_COMMITTEE']:
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
    
    # --- AUTO-CLOSE LOGIC ---
    votes = res.votes.all() if hasattr(res, 'votes') else []
    total_eligible = 0
    if scope in ['EXCO', 'EXCO_CORE', 'COMMITTEE_ALL', 'SUB_COMMITTEE']:
        if scope == 'EXCO_CORE':
            total_eligible = UipCommitteeMember.query.filter(
                UipCommitteeMember.organization_id == org.id, 
                UipCommitteeMember.status == "CURRENT",
                UipCommitteeMember.position.in_(["Chairperson", "Vice-Chairperson", "Secretary", "Treasurer"])
            ).count()
        else:
            total_eligible = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").count()
    else:
        from app.models.core import CoreOrganizationMember
        total_eligible = CoreOrganizationMember.query.filter_by(organization_id=org.id, is_active=True).count()
        
    if total_eligible > 0 and len(votes) >= total_eligible:
        yea_count = len([v for v in votes if v.vote == 'YEA'])
        nay_count = len([v for v in votes if v.vote == 'NAY'])
        
        if yea_count > nay_count:
            execute_resolution_adoption(org, res, db)
            db.session.commit()
            flash(f"Voting concluded automatically! 100% participation reached. Resolution ADOPTED.", "success")
        else:
            res.status = "REJECTED"
            db.session.commit()
            flash(f"Voting concluded automatically! 100% participation reached. Resolution REJECTED.", "warning")
            
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


@uip_bp.route("/<org_slug>/resolution/draft", methods=["GET", "POST"])
@login_required
def draft_resolution(org_slug):
    org = g.organization
    
    # Check if Secretary
    exco_check = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not exco_check:
        flash("Only active Committee Members can draft resolutions.", "error")
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
    
    exco_check = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not exco_check:
        flash("Only active Committee Members can publish resolutions.", "error")
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
        
    resolution = UipResolution.query.filter_by(id=res_id, organization_id=org.id).first_or_404()
    if resolution.status != "DRAFT":
        flash("This resolution is already published.", "warning")
    else:
        resolution.status = "PROPOSED"
        db.session.commit()
        flash("Resolution published to the Digital Committee Room! ExCo members can now vote.", "success")
        
    return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=resolution.id))


@uip_bp.route("/<org_slug>/executive-workspace")
@login_required
def exco_workspace(org_slug):
    org = g.organization
    from app.models.uip import UipResolution
    from app.models.core import CoreInteraction
    
    pending_resolutions = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").count()
    
    open_claims_count = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.status == "OPEN",
        CoreInteraction.interaction_type.in_([
            "committee_claim", "ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim", "unknown_claim"
        ])
    ).count()
    
    switch_gate = 'red' if open_claims_count > 0 else 'clear'
    if tabled_res > 0:
        switch_res = 'red'
    elif pending_resolutions > 0:
        switch_res = 'amber'
    else:
        switch_res = 'clear'
        
    return render_template(
        "program_uip/dashboards/exco_workspace.html", 
        org=org, 
        pending_resolutions=pending_resolutions,
        switch_gate=switch_gate,
        switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=pending_resolutions,
        open_claims=[None] * open_claims_count
    )
