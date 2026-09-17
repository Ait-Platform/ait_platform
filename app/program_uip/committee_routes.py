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
    )
@uip_bp.route("/<org_slug>/resolution/<int:res_id>/decide", methods=["POST"])
@login_required
def decide_resolution(org_slug, res_id):
    org = g.organization
    res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
    
    # 1. Verify Lockdown Authority (Chairman or Vice Chair)
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not current_appointment or current_appointment.position.lower() not in ["chairman", "chairperson", "chair", "vice chair", "vice chairman"]:
        flash("Only the Chairman or Vice Chairman has the authority to lock down and finalize resolutions.", "danger")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
        
    decision = request.form.get("decision")
    if decision not in ["ADOPTED", "REJECTED"]:
        abort(400)
        
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
        
    return render_template("program_uip/dashboards/resolution_edit.html", org=org, resolution=resolution)
