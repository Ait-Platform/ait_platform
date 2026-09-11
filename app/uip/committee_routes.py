from flask import render_template, g, abort, redirect, url_for, request, flash, current_app
from flask_login import login_required, current_user
from itsdangerous import URLSafeTimedSerializer

from app.extensions import db
from app.models.auth import User
from app.models.core import CoreOrganizationMember, CoreRoleAssignment, CoreRole
from app.models.uip import UipCommitteeMeeting, UipResolution
from app.models.uip_governance import UipDelegation

from . import uip_bp
from .services import audit, governance

@uip_bp.route("/<org_slug>/committee-dashboard")
@login_required
def committee_dashboard(org_slug):
    org = g.organization
    
    if "committee_member" not in g.uip_roles and "manager" not in g.uip_roles:
        abort(403)
        
    founding_meeting = UipCommitteeMeeting.query.filter_by(
        organization_id=org.id, meeting_type="FOUNDING"
    ).first()
    
    election_resolution = None
    manager_resolution = None
    elected_users = []
    current_manager = None
    
    if founding_meeting:
        election_resolution = UipResolution.query.filter_by(
            meeting_id=founding_meeting.id, title="Election of Committee Members"
        ).first()
        
        manager_resolution = UipResolution.query.filter_by(
            meeting_id=founding_meeting.id, title="Manager Designation"
        ).first()
        
        if election_resolution and election_resolution.result_basis:
            user_ids = election_resolution.result_basis.get("elected_committee_user_ids", [])
            if user_ids:
                elected_users = User.query.filter(User.id.in_(user_ids)).all()
                
        if manager_resolution and manager_resolution.responsible_user_id:
            current_manager = User.query.get(manager_resolution.responsible_user_id)
            
    delegated_responsibility = UipDelegation.query.filter_by(
        organization_id=org.id,
        delegated_user_id=current_user.id,
        status="ACTIVE"
    ).first()
    
    # Manager delegation controls
    ratepayer_admin = None
    if "manager" in g.uip_roles:
        ratepayer_admin = UipDelegation.query.filter_by(
            organization_id=org.id,
            delegation_type="RATEPAYER_ADMIN",
            status="ACTIVE"
        ).first()
        
    return render_template(
        "uip/dashboards/committee.html",
        org=org,
        founding_meeting=founding_meeting,
        election_resolution=election_resolution,
        manager_resolution=manager_resolution,
        elected_users=elected_users,
        current_manager=current_manager,
        delegated_responsibility=delegated_responsibility,
        ratepayer_admin=ratepayer_admin
    )

@uip_bp.route("/<org_slug>/delegate/ratepayer-admin", methods=["POST"])
@login_required
def delegate_ratepayer_admin(org_slug):
    org = g.organization
    if "manager" not in g.uip_roles:
        abort(403)
        
    delegated_user_id = request.form.get("delegated_user_id")
    
    # Revoke existing
    existing = UipDelegation.query.filter_by(
        organization_id=org.id,
        delegation_type="RATEPAYER_ADMIN",
        status="ACTIVE"
    ).all()
    for d in existing:
        d.status = "REVOKED"
        
    if not delegated_user_id:
        audit.record(org.id, current_user.id, "delegation.revoked", None)
        db.session.commit()
        flash("Delegation successfully revoked.", "info")
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
        
    delegated_user_id = int(delegated_user_id)
    
    # Must be an activated committee member
    is_committee = CoreRoleAssignment.query.join(CoreRole).join(CoreOrganizationMember, CoreOrganizationMember.user_id == CoreRoleAssignment.user_id).filter(
        CoreRoleAssignment.organization_id == org.id,
        CoreRoleAssignment.user_id == delegated_user_id,
        CoreRole.slug == "committee_member",
        CoreOrganizationMember.is_active == True
    ).first()
    
    if not is_committee:
        abort(400, description="User is not an active committee member.")
        
    # Create new
    new_delegation = UipDelegation(
        organization_id=org.id,
        delegated_user_id=delegated_user_id,
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
        # Check resolutions to assign roles
        meeting = UipCommitteeMeeting.query.filter_by(
            organization_id=org.id, meeting_type="FOUNDING"
        ).first()
        
        is_elected = False
        roles_to_assign = []
        if meeting:
            elec = UipResolution.query.filter_by(meeting_id=meeting.id, title="Election of Committee Members").first()
            if elec and elec.result_basis and user.id in elec.result_basis.get("elected_committee_user_ids", []):
                is_elected = True
                roles_to_assign.append("committee_member")
                
            man = UipResolution.query.filter_by(meeting_id=meeting.id, title="Manager Designation").first()
            if man and man.responsible_user_id == user.id:
                roles_to_assign.append("manager")
                
        if not is_elected:
            abort(403, description="Only elected committee members can use this activation link.")
            
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
        
    return render_template("uip/activate_committee.html", org=org, user=user)
