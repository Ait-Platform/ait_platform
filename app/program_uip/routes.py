from datetime import datetime
import random
import uuid

from flask import abort, flash, g, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from sqlalchemy.exc import IntegrityError

from app.extensions import db
from app.models.auth import User
from app.models.core import CoreInteraction, CoreOrganizationMember, CoreRole, CoreRoleAssignment, CoreTask
from app.models.uip import UipMunicipalReferral, UipProvider, UipWorkOrder
from app.program_uip import uip_bp
from app.program_uip.services import audit, register, providers, work_orders, operations
from app.program_uip.services import routing, sla, reception
from app.program_uip.services.dashboard import metrics
from app.models.uip import (UipMemberProfile, UipProperty, UipPropertyMember,
    UipMemberRepresentative, UipCommunicationPreference)


STAFF_ROLES = ("manager", "receptionist")
CATEGORIES = {"GENERAL ENQUIRY", "SERVICE ISSUE", "SECURITY", "CLEANING",
              "MAINTENANCE", "MUNICIPAL SERVICE", "COMMUNITY MATTER", "FINANCIAL"}
CHANNELS = {"Telephone", "Reception", "Email", "Web", "WhatsApp"}
PRIORITIES = {"LOW", "NORMAL", "HIGH", "URGENT"}


def _require_role(*allowed, abort_on_fail=True):
    """Require active membership and an eligible role within this organisation."""
    membership = CoreOrganizationMember.query.filter_by(
        organization_id=g.organization.id, user_id=current_user.id, is_active=True
    ).first()
    if not membership:
        if abort_on_fail:
            abort(403)
        return None
    query = CoreRoleAssignment.query.filter_by(
        organization_id=g.organization.id, user_id=current_user.id
    )
    if allowed:
        query = query.filter(CoreRoleAssignment.role.has(db.and_(CoreRole.slug.in_(allowed),
            db.or_(CoreRole.organization_id.is_(None), CoreRole.organization_id == g.organization.id))))
    query = query.filter(CoreRoleAssignment.role.has(db.or_(
        CoreRole.organization_id.is_(None), CoreRole.organization_id == g.organization.id)))
    assignments = query.all()
    roles = [assignment.role.slug for assignment in assignments if assignment.role]
    
    # Dynamically inject committee_member role based on current term records
    if not allowed or "committee_member" in allowed:
        from app.models.uip_governance import UipCommitteeMember
        from sqlalchemy import func
        from sqlalchemy.exc import ProgrammingError
        try:
            is_committee = UipCommitteeMember.query.filter(
                UipCommitteeMember.organization_id == g.organization.id,
                UipCommitteeMember.status == "CURRENT",
                func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
            ).first()
            if is_committee and "committee_member" not in roles:
                roles.append("committee_member")
        except ProgrammingError:
            db.session.rollback()

    if allowed:
        # Staff privileges take precedence over a second resident/provider role.
        roles = [role for role in allowed if role in roles]
    if not roles:
        if abort_on_fail:
            abort(403)
        return None
    return roles[0]


def _members_with_roles(*roles):
    return User.query.join(
        CoreOrganizationMember, CoreOrganizationMember.user_id == User.id
    ).join(
        CoreRoleAssignment, CoreRoleAssignment.user_id == User.id
    ).filter(
        CoreOrganizationMember.organization_id == g.organization.id,
        CoreOrganizationMember.is_active.is_(True),
        CoreRoleAssignment.organization_id == g.organization.id,
        CoreRoleAssignment.role.has(db.and_(CoreRole.slug.in_(roles),
            db.or_(CoreRole.organization_id.is_(None), CoreRole.organization_id == g.organization.id)))
    ).distinct()


def _interaction(reference):
    return CoreInteraction.query.filter_by(
        organization_id=g.organization.id, reference=reference
    ).first_or_404()


def _positive_id(value):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        abort(400, description="Invalid record ID.")
    if parsed <= 0:
        abort(400, description="Invalid record ID.")
    return parsed


@uip_bp.route("/<org_slug>/dashboard")
@login_required
def dashboard(org_slug):
    org = g.organization
    role_slug = _require_role("owner", "resident", abort_on_fail=False) if request.args.get("as_ratepayer") else _require_role("manager", "receptionist", "committee_member", "owner", "resident", "provider", "municipal_officer", abort_on_fail=False)
    
    if not role_slug:
        return redirect(url_for("uip_bp.my_access", org_slug=org_slug))
        
    if role_slug == "municipal_officer":
        return redirect(url_for("uip_bp.mo_dashboard", org_slug=org_slug))
    if role_slug == "provider":
        return redirect(url_for("uip_bp.work_order_list", org_slug=org_slug))
    if role_slug in {"resident", "owner"}:
        interactions = CoreInteraction.query.filter_by(
            organization_id=org.id, creator_id=current_user.id
        ).all()
        return render_template("program_uip/dashboards/resident.html", org=org, interactions=interactions)
    if role_slug == "receptionist":
        from app.program_uip.presentation import issue_rows
        rows = [row for row in issue_rows(org.id, current_user.id) if "open" in row["filters"]]
        return render_template("program_uip/dashboards/receptionist.html", org=org, issue_rows=rows)
    if role_slug == "committee_member":
        from app.models.uip_governance import UipCommitteeMember
        from sqlalchemy import func
        current_appointment = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org.id,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
        ).first()
        
        if current_appointment:
            pos = current_appointment.position.lower()
            if pos in ["chairman", "vice chairman", "chair", "chairperson", "vice chair"]:
                from app.program_uip.presentation import executive
                return render_template("program_uip/dashboards/manager.html", org=org, overview=executive(org.id, current_user.id))
            elif pos == "treasurer":
                return redirect(url_for("uip_bp.finance_overview", org_slug=org_slug))
            
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org_slug))
    if role_slug == "manager":
        from app.program_uip.presentation import executive
        return render_template("program_uip/dashboards/manager.html", org=org, overview=executive(org.id, current_user.id))
    
    return redirect(url_for("uip_bp.my_access", org_slug=org_slug))



@uip_bp.route("/<org_slug>/waiting-lounge")
@login_required
def waiting_lounge(org_slug):
    from flask import request, render_template, g, redirect, url_for, flash
    from flask_login import current_user
    from sqlalchemy import func
    from app.models.uip import UipCommitteeMeeting
    from app.models.uip_governance import UipCommitteeMember
    
    org = g.organization
    
    # Auto-admit if they were verified while waiting
    appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if appointment and not force_menu:
        from app.models.core import CoreOrganizationMember
        from app import db
        membership = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=current_user.id).first()
        if not membership:
            membership = CoreOrganizationMember(organization_id=org.id, user_id=current_user.id, is_active=True)
            db.session.add(membership)
        elif not membership.is_active:
            membership.is_active = True
        db.session.commit()
        flash("Your committee membership has been verified!", "success")
        pos = appointment.position.lower()
        if pos in ['chairman', 'chairperson', 'chair', 'vice chair', 'vice chairman', 'treasurer']:
            return redirect(url_for('uip_bp.dashboard', org_slug=org.slug))
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
        
    claim = request.args.get("claim", "unknown")
    
    founding_exists = UipCommitteeMeeting.query.filter_by(
        organization_id=org.id, meeting_type="FOUNDING"
    ).first() is not None
    
    return render_template("program_uip/waiting_lounge.html", org=org, claim=claim, founding_exists=founding_exists)

@uip_bp.route("/<org_slug>/waiting-lounge/dispute", methods=["POST"])
@login_required
def waiting_lounge_dispute(org_slug):
    from flask import flash, redirect, url_for, g
    from flask_login import current_user
    from app.models.core import CoreInteraction
    from app import db
    
    interaction = CoreInteraction(
        organization_id=g.organization.id,
        creator_id=current_user.id,
        interaction_type="committee_dispute",
        title="Committee Membership Verification Dispute",
        description="User disputes their missing committee verification status.",
        status="OPEN"
    )
    db.session.add(interaction)
    db.session.commit()
    
    flash("Dispute lodged successfully. The administration team will review your status.", "success")
    return redirect(url_for('uip_bp.my_access', org_slug=org_slug, claim='committee_nomatch'))

@uip_bp.route("/<org_slug>/router")
@login_required
def router_page(org_slug):
    org = g.organization
    from flask import request, redirect, url_for
    from flask_login import current_user
    from sqlalchemy import func
    force_menu = request.args.get('force')
    
    # 1. Auto-route if already verified committee
    from app.models.uip_governance import UipCommitteeMember
    appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    if appointment and not force_menu:
        from app.models.core import CoreOrganizationMember
        from app import db
        membership = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=current_user.id).first()
        if not membership:
            membership = CoreOrganizationMember(organization_id=org.id, user_id=current_user.id, is_active=True)
            db.session.add(membership)
        elif not membership.is_active:
            membership.is_active = True
        db.session.commit()
        pos = appointment.position.lower()
        if pos in ['chairman', 'chairperson', 'chair', 'vice chair', 'vice chairman', 'treasurer']:
            return redirect(url_for('uip_bp.dashboard', org_slug=org.slug))
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
        
    # 1b. Auto-route if active Ratepayer
    from app.models.core import CoreOrganizationMember
    membership = CoreOrganizationMember.query.filter_by(
        organization_id=org.id, user_id=current_user.id, is_active=True
    ).first()
    
    if membership and not force_menu:
        # Since they don't have a committee appointment, but they are an active member,
        # they are a Ratepayer (or other standard role).
        return redirect(url_for("uip_bp.dashboard", org_slug=org.slug))

    # 2. Strangers / Unverified Users
    # We now always show the 7 tiles. The verify routes will handle routing to provisioning vs waiting lounge based on founding_exists.
    return render_template("program_uip/router.html", org=org)

@uip_bp.route("/<org_slug>/my-access")
@login_required
def my_access(org_slug):
    org = g.organization
    from flask import request, redirect, url_for
    from flask_login import current_user
    from app.models.core import CoreInteraction
    from app.models.uip import UipCommitteeMeeting
    
    # 1. Fetch all OPEN claims for this user to display in the Access Table
    open_claims = CoreInteraction.query.filter_by(
        organization_id=org.id, creator_id=current_user.id, status="OPEN"
    ).all()
    
    # Create a simple list of claim types
    user_claims = [c.interaction_type for c in open_claims]
    
    # Check if a Founding Meeting exists
    founding_exists = UipCommitteeMeeting.query.filter_by(
        organization_id=org.id, meeting_type="FOUNDING"
    ).first() is not None

    return render_template("program_uip/my_access.html", org=org, user_claims=user_claims, founding_exists=founding_exists)


@uip_bp.route("/<org_slug>/verify/ratepayer", methods=["GET", "POST"])
@login_required
def verify_ratepayer(org_slug):
    # Check if they already have authority
    role = _require_role("owner", "resident", abort_on_fail=False)
    if role:
        return redirect(url_for("uip_bp.dashboard", org_slug=org_slug, as_ratepayer=1))
    
    from app.models.core import CoreInteraction
    from app import db
    from flask_login import current_user
    
    claim = CoreInteraction.query.filter_by(
        organization_id=g.organization.id, creator_id=current_user.id, interaction_type="ratepayer_claim", status="OPEN"
    ).first()
    if not claim:
        claim = CoreInteraction(
            organization_id=g.organization.id, creator_id=current_user.id,
            interaction_type="ratepayer_claim", title="Ratepayer Claim",
            description=f"User {current_user.email} claims to be a ratepayer.", status="OPEN"
        )
        db.session.add(claim)
        db.session.commit()
    
    return redirect(url_for("uip_bp.my_access", org_slug=org_slug, claim="ratepayer"))


@uip_bp.route("/<org_slug>/verify/secretary", methods=["GET"])
@login_required
def verify_secretary(org_slug):
    org = g.organization
    from flask import request, redirect, url_for, flash
    from flask_login import current_user
    from app import db
    from app.models.core import CoreInteraction
    from app.models.uip import UipCommitteeMeeting
    
    # Check if a Founding Meeting exists
    founding_exists = UipCommitteeMeeting.query.filter_by(
        organization_id=org.id, meeting_type="FOUNDING"
    ).first() is not None
    
    if founding_exists:
        # The UIP is already founded. Block new users from clicking Secretary tile.
        flash("The Secretary position has already been officially designated. Please select a different participant role.", "warning")
        return redirect(url_for("uip_bp.router_page", org_slug=org.slug, force=1))
        
    # If not founded, create the claim to allow them into provisioning
    claim = CoreInteraction.query.filter_by(
        organization_id=org.id,
        creator_id=current_user.id,
        interaction_type="secretary_claim",
        status="OPEN"
    ).first()
    
    if not claim:
        claim = CoreInteraction(
            organization_id=org.id,
            creator_id=current_user.id,
            interaction_type="secretary_claim",
            title="Secretary Claim",
            description=f"User {current_user.email} claims the Secretary role.",
            status="OPEN"
        )
        db.session.add(claim)
        db.session.commit()
        
    return redirect(url_for("uip_bp.provisioning", org_slug=org.slug))

@uip_bp.route("/<org_slug>/verify/committee", methods=["GET", "POST"])
@login_required
def verify_committee(org_slug):
    org = g.organization
    
    from flask import request, redirect, url_for, flash
    from flask_login import current_user
    from sqlalchemy import func
    from sqlalchemy.exc import ProgrammingError
    from app.models.uip_governance import UipCommitteeTerm, UipCommitteeMember
    from app import db
    from app.models.core import CoreInteraction
    
    try:
        term = UipCommitteeTerm.query.filter_by(organization_id=org.id).first()
        
        # Rule 1: One Claim at a Time
        existing_claim = CoreInteraction.query.filter_by(
            organization_id=org.id,
            creator_id=current_user.id,
            interaction_type="committee_claim",
            status="OPEN"
        ).first()
        
        if existing_claim and request.method == "POST":
            flash("You already have a pending request. If you made a mistake, please click the 'Unsure / Other' tile.", "warning")
            return redirect(url_for("uip_bp.router_page", org_slug=org.slug))

        if request.method == "GET":
            # For Subcommittees (or general GETs)
            return render_template("program_uip/claim_committee.html", org=org)
            
        if request.method == "POST" and not existing_claim:
            level = request.form.get("level", "Unknown Level")
            position = request.form.get("position", "Committee Member").strip()
            portfolio = request.form.get("portfolio", "").strip()
            
            # Rule 2: Seat Occupied Fallback
            # Only check for specific singular roles
            singular_roles = ["chairman", "vice chairman", "secretary", "treasurer"]
            if position.lower() in singular_roles:
                occupied = UipCommitteeMember.query.filter(
                    UipCommitteeMember.organization_id == org.id,
                    func.lower(UipCommitteeMember.position) == position.lower(),
                    UipCommitteeMember.is_active == True
                ).first()
                if occupied:
                    flash(f"The position of {position} is currently occupied. Please click the 'Unsure / Other' tile and the Secretary will sort it out.", "warning")
                    return redirect(url_for("uip_bp.router_page", org_slug=org.slug))
            
            title = f"{level} Claim - {position}"
            desc = f"User {current_user.email} claims to be {position} on the {level}."
            if portfolio:
                title += f" ({portfolio})"
                desc += f" Portfolio: {portfolio}."
                
            claim = CoreInteraction(
                organization_id=org.id,
                creator_id=current_user.id,
                interaction_type="committee_claim",
                title=title,
                description=desc,
                status="OPEN"
            )
            db.session.add(claim)
            db.session.commit()
            
        from app.models.uip import UipCommitteeMeeting
        founding_exists = UipCommitteeMeeting.query.filter_by(
            organization_id=org.id, meeting_type="FOUNDING"
        ).first() is not None
        
        if not founding_exists:
            return redirect(url_for("uip_bp.waiting_lounge", org_slug=org.slug, claim="genesis"))
        else:
            return redirect(url_for("uip_bp.waiting_lounge", org_slug=org.slug, claim="committee_claim"))
            
    except ProgrammingError:
        return redirect(url_for("uip_bp.provisioning", org_slug=org.slug))

@uip_bp.route("/<org_slug>/verify/mo", methods=["GET", "POST"])
@login_required
def verify_mo(org_slug):
    # Check if they already have authority
    role = _require_role("municipal_officer", abort_on_fail=False)
    if role:
        return redirect(url_for("uip_bp.mo_dashboard", org_slug=org_slug))
        
    from app.models.core import CoreInteraction
    from app import db
    from flask_login import current_user
    
    claim = CoreInteraction.query.filter_by(
        organization_id=g.organization.id, creator_id=current_user.id, interaction_type="mo_claim", status="OPEN"
    ).first()
    if not claim:
        claim = CoreInteraction(
            organization_id=g.organization.id, creator_id=current_user.id,
            interaction_type="mo_claim", title="Municipal Officer Claim",
            description=f"User {current_user.email} claims to be a municipal officer.", status="OPEN"
        )
        db.session.add(claim)
        db.session.commit()
        
    return redirect(url_for("uip_bp.my_access", org_slug=org_slug, claim="mo"))


@uip_bp.route("/<org_slug>/mo-dashboard")
@login_required
def mo_dashboard(org_slug):
    org = g.organization
    _require_role("municipal_officer")
    
    from app.models.uip import UipMunicipalReferral
    from app.models.core import CoreInteraction
    escalations = UipMunicipalReferral.query.filter_by(
        organization_id=org.id,
        status="ESCALATED_TO_MO"
    ).all()
    
    return render_template("program_uip/dashboards/municipal_officer.html", org=org, escalations=escalations)

@uip_bp.route("/<org_slug>/mo-resolve/<int:referral_id>", methods=["POST"])
@login_required
def mo_resolve_ticket(org_slug, referral_id):
    org = g.organization
    _require_role("municipal_officer")
    
    from app.models.uip import UipMunicipalReferral
    from app.models.core import CoreInteraction
    from app import db
    import datetime
    
    referral = UipMunicipalReferral.query.filter_by(id=referral_id, organization_id=org.id).first_or_404()
    referral.status = "RESOLVED"
    
    # Cascade to master ticket
    master = referral.interaction
    master.status = "RESOLVED"
    master.closed_at = datetime.datetime.utcnow()
    master.closed_by = current_user.id
    
    # Auto-cascade to all collated children
    if hasattr(master, "children"):
        for child in master.children:
            child.status = "RESOLVED"
            child.closed_at = datetime.datetime.utcnow()
            child.closed_by = current_user.id
            
    db.session.commit()
    from flask import flash, redirect, url_for
    flash(f"Ticket {master.id} resolved. Auto-cascaded response to all affected ratepayers.", "success")
    return redirect(url_for("uip_bp.mo_dashboard", org_slug=org.slug))


@uip_bp.route("/<org_slug>/verify/subcommittee", methods=["GET", "POST"])
@login_required
def verify_subcommittee(org_slug):
    role = _require_role("subcommittee_member", abort_on_fail=False)
    if role:
        return redirect(url_for("uip_bp.subcommittee_dashboard", org_slug=org_slug))
    
    from app.models.core import CoreInteraction
    from app import db
    from flask_login import current_user
    
    claim = CoreInteraction.query.filter_by(
        organization_id=g.organization.id, creator_id=current_user.id, interaction_type="subcommittee_claim", status="OPEN"
    ).first()
    if not claim:
        claim = CoreInteraction(
            organization_id=g.organization.id, creator_id=current_user.id,
            interaction_type="subcommittee_claim", title="Subcommittee Member Claim",
            description=f"User {current_user.email} claims to be a subcommittee member.", status="OPEN"
        )
        db.session.add(claim)
        db.session.commit()
        
    return redirect(url_for("uip_bp.my_access", org_slug=org_slug, claim="subcommittee"))

@uip_bp.route("/<org_slug>/subcommittee-dashboard")
@login_required
def subcommittee_dashboard(org_slug):
    _require_role("subcommittee_member")
    return render_template("program_uip/dashboards/subcommittee.html", org=g.organization)


@uip_bp.route("/<org_slug>/verify/staff", methods=["GET", "POST"])
@login_required
def verify_staff(org_slug):
    # Check if they have an operational role
    role = _require_role("manager", "receptionist", "provider", abort_on_fail=False)
    
    from sqlalchemy.exc import ProgrammingError
    from app.models.uip_governance import UipDelegation
    try:
        ratepayer_admin = UipDelegation.query.filter_by(
            organization_id=g.organization.id,
            delegation_type="RATEPAYER_ADMIN",
            status="ACTIVE",
            delegated_user_id=current_user.id
        ).first()
    except ProgrammingError:
        db.session.rollback()
        ratepayer_admin = None
    
    if role == "manager":
        return redirect(url_for("uip_bp.dashboard", org_slug=org_slug)) # Dashboard handles manager route
    if role == "receptionist":
        return redirect(url_for("uip_bp.dashboard", org_slug=org_slug))
    if role == "provider":
        return redirect(url_for("uip_bp.work_order_list", org_slug=org_slug))
    if ratepayer_admin:
        return redirect(url_for("uip_bp.member_list", org_slug=org_slug))
        
    from app.models.core import CoreInteraction
    
    claim = CoreInteraction.query.filter_by(
        organization_id=g.organization.id, creator_id=current_user.id, interaction_type="staff_claim", status="OPEN"
    ).first()
    if not claim:
        claim = CoreInteraction(
            organization_id=g.organization.id, creator_id=current_user.id,
            interaction_type="staff_claim", title="Staff / Provider Claim",
            description=f"User {current_user.email} claims to be staff or service provider.", status="OPEN"
        )
        db.session.add(claim)
        db.session.commit()
        
    return redirect(url_for("uip_bp.my_access", org_slug=org_slug, claim="staff"))


@uip_bp.route("/<org_slug>/verify/public", methods=["GET", "POST"])
def verify_public(org_slug):
    # Public reporters don't need roles. Just go to intake.
    return redirect(url_for("uip_bp.public_dashboard", org_slug=org_slug))


@uip_bp.route("/<org_slug>/public-dashboard")
def public_dashboard(org_slug):
    # Placeholder for public reporting intake. No role checks required!
    return render_template("program_uip/dashboards/public.html", org=g.organization)


@uip_bp.route("/<org_slug>/settings", methods=["GET", "POST"])
@login_required
def org_settings(org_slug):
    _require_role("manager", "committee_member", "owner")
    org = g.organization
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        status = request.form.get("status")
        if not name or len(name) > 255 or status not in {"active", "inactive"}:
            abort(400, description="Invalid organisation name or status.")
        values = {}
        for field, limit in (("area", 255), ("municipality_ref", 255),
                             ("contact_email", 255), ("contact_phone", 50)):
            values[field] = (request.form.get(field) or "").strip()
            if len(values[field]) > limit:
                abort(400, description="Organisation detail is too long.")
        org.name, org.status = name, status
        for field, value in values.items():
            setattr(org, field, value)
        audit.record(org.id, current_user.id, "organization.updated", org)
        db.session.commit()
        flash("Organisation settings updated successfully.", "success")
        return redirect(url_for("uip_bp.org_settings", org_slug=org_slug))
    return render_template("program_uip/admin/settings.html", org=org)


@uip_bp.route("/<org_slug>/interaction/new", methods=["GET", "POST"])
@login_required
def new_interaction(org_slug):
    _require_role("manager", "receptionist", "committee_member")
    g.intake_title_presets = reception.SHORT_TITLE_PRESETS
    org = g.organization
    residents = _members_with_roles("resident").all()
    register_members = register.members(org.id, current_user.id, active_only=True).all()
    register_properties = register.properties(org.id, current_user.id, active_only=True).all()
    selected_member, selected_property = register.intake_links(org.id, current_user.id,
        request.args.get("member_id"), request.args.get("property_id"))
    g.intake_member_id = selected_member.id if selected_member else None
    g.intake_property_id = selected_property.id if selected_property else None
    from app.program_uip.presentation import current_relationship
    g.intake_property_members = {}
    intake_relationships = []
    for relationship in UipPropertyMember.query.filter_by(organization_id=org.id).all():
        if current_relationship(relationship):
            g.intake_property_members.setdefault(relationship.property_id, []).append(relationship.member_id)
            intake_relationships.append(dict(member_id=relationship.member_id,
                property_id=relationship.property_id, relationship=relationship.relationship,
                verified=relationship.is_verified))
    # Only authorized, active records from this organisation enter the intake view.
    member_ids = {m.id for m in register_members}
    property_ids = {p.id for p in register_properties}
    g.intake_register = dict(
        members=[dict(id=m.id, name=m.name, reference=m.reference, email=m.email,
                      phone=m.phone) for m in register_members],
        properties=[dict(id=p.id, reference=p.reference, address=p.address,
                         rates_reference=p.rates_reference) for p in register_properties],
        relationships=[r for r in intake_relationships
                       if r['member_id'] in member_ids and r['property_id'] in property_ids])
    if selected_member:
        register_properties.sort(key=lambda p: selected_member.id not in g.intake_property_members.get(p.id, []))
    if request.method == "POST":
        email = (request.form.get("resident_email") or "").strip()
        resident = next((member for member in residents if member.email == email), None)
        if email and not resident:
            abort(400, description="Select an active resident of this organisation.")
        member, property_record = register.intake_links(
            org.id, current_user.id, request.form.get("member_id"), request.form.get("property_id"))
        if resident and member and member.membership.user_id != resident.id:
            abort(400, description="The selected member and resident account do not match.")
        title = (request.form.get("title") or "").strip()
        description = (request.form.get("description") or "").strip()
        category = request.form.get("category")
        channel = request.form.get("channel")
        priority = request.form.get("priority")
        if (not title or len(title) > 255 or not description
                or category not in CATEGORIES or channel not in CHANNELS
                or priority not in PRIORITIES):
            abort(400, description="Complete the required issue fields with valid values.")
        for attempt in range(5):
            ref = f"{org.slug[:2].upper()}-{random.randint(10000, 99999)}"
            ix = CoreInteraction(
                reference=ref, organization_id=org.id,
                creator_id=resident.id if resident else current_user.id,
                title=title, description=description, channel=channel,
                category=category, interaction_type=category, priority=priority, status="NEW",
                member_id=member.id if member else None,
                property_id=property_record.id if property_record else None,
                recorded_by=current_user.id
            )
            db.session.add(ix)
            try:
                audit.record(org.id, current_user.id, "interaction.created", ix)
                sla.intake(ix)
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                # Retry only reference collisions; do not conceal other database errors.
                if not CoreInteraction.query.filter_by(reference=ref).first():
                    raise
            else:
                flash(f"Interaction {ref} logged successfully.", "success")
                return redirect(url_for("uip_bp.view_interaction", org_slug=org.slug, reference=ix.reference))
        flash("Could not allocate an issue reference. Please try again.", "warning")
        return render_template("program_uip/reception/new_interaction.html", org=org, residents=residents, register_members=register_members, register_properties=register_properties), 409
    return render_template("program_uip/reception/new_interaction.html", org=org, residents=residents, register_members=register_members, register_properties=register_properties)


@uip_bp.route("/<org_slug>/interaction/<reference>")
@login_required
def view_interaction(org_slug, reference):
    role = _require_role("manager", "receptionist", "committee_member", "resident")
    ix = _interaction(reference)
    if role == "resident" and ix.creator_id != current_user.id:
        abort(403)
    return render_template("program_uip/interactions/view.html", org=g.organization,
                           interaction=ix, staff=_members_with_roles(*STAFF_ROLES).all(),
                           current_role=role, task_actionable=operations.actionable, request_key=str(uuid.uuid4()),
                           eligible_providers=[r["provider"] for r in routing.recommend(g.organization.id, current_user.id, ix.id)] if role in STAFF_ROLES else [],
                           register_member=UipMemberProfile.query.filter_by(id=ix.member_id, organization_id=g.organization.id).first() if ix.member_id else None,
                           register_property=UipProperty.query.filter_by(id=ix.property_id, organization_id=g.organization.id).first() if ix.property_id else None)


@uip_bp.route("/<org_slug>/interaction/<reference>/task", methods=["POST"])
@login_required
def add_task(org_slug, reference):
    _require_role("manager", "receptionist")
    ix = _interaction(reference)
    title = (request.form.get("title") or "").strip()
    if not title or len(title) > 255:
        abort(400, description="A valid task title is required.")
    assignee_id = request.form.get("assignee_id")
    if assignee_id:
        assignee_id = _positive_id(assignee_id)
        if not _members_with_roles(*STAFF_ROLES).filter(User.id == assignee_id).first():
            abort(400, description="Select active staff from this organisation.")
    task = operations.add_task(g.organization.id, current_user.id, ix.id, title,
                               request.form.get("description"), assignee_id or None)
    db.session.commit()
    flash("Task added successfully.", "success")
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=reference))


@uip_bp.route("/<org_slug>/task/<int:task_id>/complete", methods=["POST"])
@login_required
def complete_task(org_slug, task_id):
    task = operations.finish_task(g.organization.id, current_user.id, task_id,
                                  expected=request.form.get("expected_version"))
    db.session.commit()
    flash("Task marked as completed.", "success")
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=task.interaction.reference))


@uip_bp.route("/<org_slug>/interaction/<reference>/resolve", methods=["POST"])
@login_required
def resolve_interaction(org_slug, reference):
    _require_role("manager", "receptionist")
    ix = _interaction(reference)
    operations.resolve(g.organization.id, current_user.id, ix.id)
    db.session.commit()
    flash("Interaction resolved successfully.", "success")
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=reference))


@uip_bp.route("/<org_slug>/interaction/<reference>/summarize", methods=["POST"])
@login_required
def summarize_interaction(org_slug, reference):
    _require_role("manager", "receptionist")
    ix = _interaction(reference)
    return redirect(url_for("uip_bp.ai_assistant", org_slug=org_slug, feature="issue", issue_id=ix.id))


@uip_bp.route("/<org_slug>/interaction/<reference>/provider", methods=["POST"])
@login_required
def assign_provider(org_slug, reference):
    _require_role(*STAFF_ROLES)
    ix = _interaction(reference)
    provider_id = _positive_id(request.form.get("provider_id"))
    if not UipProvider.query.filter_by(id=provider_id, organization_id=g.organization.id).first():
        abort(400, description="Select a provider from this organisation.")
    order = work_orders.create(g.organization.id, current_user.id, ix.id, provider_id,
        request.form.get("description"), request.form.get("service_location"), request.form.get("request_key"))
    db.session.commit()
    flash("Work order created. Dispatch has not yet been recorded.", "success")
    return redirect(url_for("uip_bp.work_order_view", org_slug=org_slug, order_id=order.id))


@uip_bp.route("/<org_slug>/interaction/<reference>/municipal", methods=["POST"])
@login_required
def escalate_municipality(org_slug, reference):
    _require_role("manager", "receptionist", "committee_member")
    ix = _interaction(reference)
    department = (request.form.get("department") or "").strip()
    mun_ref = (request.form.get("municipality_reference") or "").strip()
    if not department or len(department) > 100 or len(mun_ref) > 100:
        abort(400, description="A valid department and reference are required.")
    referral = reception.referral(g.organization.id, current_user.id, ix.id, department, reference=mun_ref)
    if ix.status == "NEW":
        ix.status = "IN_PROGRESS"
    db.session.commit()
    flash(f"Referral recorded ({department}).", "info")
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=reference))


@uip_bp.route("/<org_slug>/reports")
@login_required
def org_reports(org_slug):
    role = _require_role("manager", "committee_member")
    from app.program_uip.services.governance import overview
    report = metrics(g.organization.id, current_user.id) if role == "manager" else overview(g.organization.id, current_user.id)
    return render_template("program_uip/dashboards/reports.html", org=g.organization, report=report, report_role=role)


@uip_bp.route("/<org_slug>/reports/generate_ai", methods=["POST"])
@login_required
def generate_ai_report(org_slug):
    _require_role("manager", "committee_member")
    return redirect(url_for("uip_bp.ai_assistant", org_slug=org_slug, feature="activity"))


@uip_bp.route("/")
def uip_start():
    from app.models.core import CoreOrganization, CoreOrganizationEntitlement
    from app.models.uip import UipCommitteeMeeting
    from app.models.auth import AuthSubject
    from app import db
    
    uip_subj = AuthSubject.query.filter_by(slug='uip').first()
    
    # Self-heal missing entitlements for ALL orgs
    all_orgs = CoreOrganization.query.all()
    for org in all_orgs:
        if uip_subj:
            ent = CoreOrganizationEntitlement.query.filter_by(organization_id=org.id, subject_id=uip_subj.id).first()
            if not ent:
                ent = CoreOrganizationEntitlement(organization_id=org.id, subject_id=uip_subj.id, status="active", is_trial=True)
                db.session.add(ent)
                
    # Clean up duplicate Manor Gardens
    manor_orgs = CoreOrganization.query.filter(
        CoreOrganization.name.ilike('%Manor Gardens%'),
        CoreOrganization.status == "active"
    ).all()
    
    if len(manor_orgs) > 1:
        # Find the one that actually has a founding meeting (the real one)
        real_org = None
        for org in manor_orgs:
            if UipCommitteeMeeting.query.filter_by(organization_id=org.id).first():
                real_org = org
                break
        
        # If we found the real one, mark the others as deleted
        if real_org:
            for org in manor_orgs:
                if org.id != real_org.id:
                    org.status = "deleted"
                    
    db.session.commit()
    
    # Fast-path: If the user is already a member of exactly one active UIP, jump straight to it!
    from flask_login import current_user
    from flask import redirect, url_for
    if current_user.is_authenticated:
        from app.models.core import CoreOrganizationMember
        memberships = CoreOrganizationMember.query.filter_by(user_id=current_user.id).all()
        if memberships:
            memberships.sort(key=lambda m: not m.is_active)
            primary_org_id = memberships[0].organization_id
            primary_org = CoreOrganization.query.get(primary_org_id)
            if primary_org and primary_org.status == 'active':
                return redirect(url_for('uip_bp.router_page', org_slug=primary_org.slug))
        
        # Also check if they have any OPEN claims!
        from app.models.core import CoreInteraction
        claim = CoreInteraction.query.filter_by(
            creator_id=current_user.id,
            status='OPEN'
        ).first()
        if claim and claim.organization_id:
            claim_org = CoreOrganization.query.get(claim.organization_id)
            if claim_org and claim_org.status == 'active':
                return redirect(url_for('uip_bp.router_page', org_slug=claim_org.slug))

    # Get all active organizations that might be UIPs.
    orgs = CoreOrganization.query.filter_by(status="active").order_by(CoreOrganization.name).all()
    return render_template('program_uip/public_about.html', orgs=orgs)

@uip_bp.route("/select", methods=["POST"])
def select_org():
    from flask import request, redirect, url_for, flash
    from flask_login import current_user
    org_slug = request.form.get("org_slug")
    if org_slug:
        next_url = url_for("uip_bp.router_page", org_slug=org_slug)
        if not current_user.is_authenticated:
            return redirect(url_for("auth_bp.register", next=next_url))
        return redirect(next_url)
    flash("Please select a valid precinct.", "warning")
    return redirect(url_for("uip_bp.uip_start"))

@uip_bp.route("/price")
def price_page():
    return redirect(url_for('auth_bp.register', subject='uip'))


@uip_bp.errorhandler(IntegrityError)
def register_conflict(error):
    db.session.rollback()
    return render_template("program_uip/validation_error.html", org=getattr(g, "organization", None),
        error="The record conflicts with an existing reference or relationship. Review the values and retry."), 409


def _register_context():
    from app.program_uip.services.register import require_register_admin
    can_manage = False
    try:
        require_register_admin(g.organization.id, current_user.id)
        can_manage = True
    except Exception:
        pass
    return dict(org=g.organization, can_manage=can_manage)


@uip_bp.route("/<org_slug>/members")
@login_required
def member_list(org_slug):
    query = register.members(g.organization.id, current_user.id)
    search = request.args.get("q", "").strip()[:120]
    status = request.args.get("status", "all")
    if search:
        query = query.filter(db.or_(UipMemberProfile.name.ilike(f"%{search}%"), UipMemberProfile.reference.ilike(f"%{search}%"), UipMemberProfile.email.ilike(f"%{search}%"), UipMemberProfile.phone.ilike(f"%{search}%")))
    if status in {"active", "inactive"}:
        query = query.filter_by(is_active=status == "active")
    page = query.paginate(
        page=request.args.get("page", 1, type=int), per_page=30, error_out=False)
    from app.program_uip.presentation import register_links, current_relationship
    members, properties, links = register_links(g.organization.id)
    linked = {m.id: [properties[l.property_id] for l in links if l.member_id == m.id and current_relationship(l)] for m in page.items}
    return render_template("program_uip/members/list.html", page=page, linked=linked, search=search, status=status, **_register_context())


@uip_bp.route("/<org_slug>/members/<int:member_id>/edit", methods=["GET", "POST"])
@login_required
def member_form(org_slug, member_id):
    from app.program_uip.services.register import require_register_admin
    require_register_admin(g.organization.id, current_user.id)
    member = register.get(UipMemberProfile, g.organization.id, current_user.id, member_id)
    if request.method == "POST":
        member = register.save_member(g.organization.id, current_user.id, request.form, member_id)
        db.session.commit()
        flash("Member register saved. Application access and roles are unchanged.", "success")
        if not member_id and request.args.get("return_to") == "intake":
            return redirect(url_for("uip_bp.new_interaction", org_slug=org_slug, member_id=member.id,
                                    property_id=request.args.get("property_id", type=int)))
        return redirect(url_for("uip_bp.member_view", org_slug=org_slug, member_id=member.id))
    return render_template("program_uip/members/form.html", member=member,
        account_names={u.id: u.name or u.email or str(u.id) for u in User.query.join(
            CoreOrganizationMember, CoreOrganizationMember.user_id == User.id).filter(
                CoreOrganizationMember.organization_id == g.organization.id,
                CoreOrganizationMember.is_active.is_(True)).all()},
        memberships=register.available_memberships(g.organization.id, current_user.id), **_register_context())


@uip_bp.route("/<org_slug>/members/<int:member_id>")
@login_required
def member_view(org_slug, member_id):
    member = register.get(UipMemberProfile, g.organization.id, current_user.id, member_id)
    return render_template("program_uip/members/view.html", member=member,
        members=register.members(g.organization.id, current_user.id).all(),
        ownerships=UipPropertyMember.query.filter_by(organization_id=g.organization.id, member_id=member.id).all(),
        representations=UipMemberRepresentative.query.filter_by(organization_id=g.organization.id, member_id=member.id).all(),
        preferences=UipCommunicationPreference.query.filter_by(organization_id=g.organization.id, member_id=member.id).all(),
        properties=register.properties(g.organization.id, current_user.id, active_only=True).all(),
        prior_issues=CoreInteraction.query.filter_by(organization_id=g.organization.id, member_id=member.id).order_by(CoreInteraction.id.desc()).all(),
        channels=register.CHANNELS, **_register_context())


@uip_bp.route("/<org_slug>/members/<int:member_id>/representatives", methods=["POST"])
@uip_bp.route("/<org_slug>/members/<int:member_id>/representatives/<int:link_id>", methods=["POST"])
@login_required
def member_representative(org_slug, member_id, link_id=None):
    register.save_relationship(g.organization.id, current_user.id, request.form, member_id=member_id, link_id=link_id)
    db.session.commit()
    flash("Representation recorded; no voting or governance rights were granted.", "success")
    return redirect(url_for("uip_bp.member_view", org_slug=org_slug, member_id=member_id))


@uip_bp.route("/<org_slug>/members/<int:member_id>/preferences", methods=["POST"])
@login_required
def member_preference(org_slug, member_id):
    register.set_preference(g.organization.id, current_user.id, member_id, request.form)
    db.session.commit()
    flash("Communication preference recorded. No message was sent.", "success")
    return redirect(url_for("uip_bp.member_view", org_slug=org_slug, member_id=member_id))


@uip_bp.route("/<org_slug>/properties")
@login_required
def property_list(org_slug):
    query = register.properties(g.organization.id, current_user.id)
    search = request.args.get("q", "").strip()[:120]
    status = request.args.get("status", "all")
    if search:
        query = query.filter(db.or_(UipProperty.address.ilike(f"%{search}%"), UipProperty.reference.ilike(f"%{search}%"), UipProperty.rates_reference.ilike(f"%{search}%")))
    if status in {"active", "inactive"}:
        query = query.filter_by(is_active=status == "active")
    page = query.paginate(
        page=request.args.get("page", 1, type=int), per_page=30, error_out=False)
    from app.program_uip.presentation import register_links, current_relationship
    members, properties, links = register_links(g.organization.id)
    linked = {p.id: [members[l.member_id] for l in links if l.property_id == p.id and current_relationship(l)] for p in page.items}
    return render_template("program_uip/properties/list.html", page=page, linked=linked, search=search, status=status, **_register_context())


@uip_bp.route("/<org_slug>/properties/<int:property_id>/edit", methods=["GET", "POST"])
@login_required
def property_form(org_slug, property_id):
    from app.program_uip.services.register import require_register_admin
    require_register_admin(g.organization.id, current_user.id)
    item = register.get(UipProperty, g.organization.id, current_user.id, property_id)
    if request.method == "POST":
        item = register.save_property(g.organization.id, current_user.id, request.form, property_id)
        db.session.commit()
        flash("Property saved.", "success")
        return redirect(url_for("uip_bp.property_view", org_slug=org_slug, property_id=item.id, member_id=request.args.get("member_id", type=int)))
    return render_template("program_uip/properties/form.html", item=item, **_register_context())


@uip_bp.route("/<org_slug>/properties/<int:property_id>")
@login_required
def property_view(org_slug, property_id):
    item = register.get(UipProperty, g.organization.id, current_user.id, property_id)
    return render_template("program_uip/properties/view.html", item=item,
        members=register.members(g.organization.id, current_user.id).all(),
        ownerships=UipPropertyMember.query.filter_by(organization_id=g.organization.id, property_id=item.id).all(),
        prior_issues=CoreInteraction.query.filter_by(organization_id=g.organization.id, property_id=item.id).order_by(CoreInteraction.id.desc()).all(),
        **_register_context())


@uip_bp.route("/<org_slug>/properties/<int:property_id>/members", methods=["POST"])
@uip_bp.route("/<org_slug>/properties/<int:property_id>/members/<int:link_id>", methods=["POST"])
@login_required
def property_member(org_slug, property_id, link_id=None):
    register.save_relationship(g.organization.id, current_user.id, request.form, property_id=property_id, link_id=link_id)
    db.session.commit()
    flash("Property relationship recorded; eligibility is maintained separately.", "success")
    return redirect(url_for("uip_bp.property_view", org_slug=org_slug, property_id=property_id))


@uip_bp.route("/<org_slug>/audit")
@login_required
def audit_history(org_slug):
    page = audit.events(g.organization.id, current_user.id).paginate(
        page=request.args.get("page", 1, type=int), per_page=30, error_out=False)
    return render_template("program_uip/audit/list.html", org=g.organization, page=page, event=None)


@uip_bp.route("/<org_slug>/audit/<int:event_id>")
@login_required
def audit_event(org_slug, event_id):
    event = audit.events(g.organization.id, current_user.id, event_id=event_id)
    return render_template("program_uip/audit/list.html", org=g.organization, page=None, event=event)


@uip_bp.route("/<org_slug>/task/<int:task_id>/cancel", methods=["POST"])
@login_required
def cancel_task(org_slug, task_id):
    task = operations.finish_task(g.organization.id, current_user.id, task_id, cancel=True,
        reason=request.form.get("reason"), expected=request.form.get("expected_version"))
    db.session.commit()
    flash("Internal task cancelled; its history is retained.", "success")
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=task.interaction.reference))


@uip_bp.route("/<org_slug>/providers")
@login_required
def provider_list(org_slug):
    _require_role("manager")
    rows = UipProvider.query.filter_by(organization_id=g.organization.id).order_by(UipProvider.name).all()
    from app.models.uip import UipProviderCapability
    caps = UipProviderCapability.query.filter_by(organization_id=g.organization.id).all()
    active_work = dict(db.session.query(UipWorkOrder.provider_id, db.func.count(UipWorkOrder.id)).filter(
        UipWorkOrder.organization_id == g.organization.id, UipWorkOrder.status.notin_(providers.TERMINAL)).group_by(UipWorkOrder.provider_id).all())
    return render_template("program_uip/providers/list.html", org=g.organization, providers=rows,
        services={p.id: [c.category for c in caps if c.provider_id == p.id] for p in rows}, active_work=active_work)


@uip_bp.route("/<org_slug>/providers/new", methods=["GET", "POST"])
@uip_bp.route("/<org_slug>/providers/<int:provider_id>/edit", methods=["GET", "POST"])
@login_required
def provider_form(org_slug, provider_id=None):
    _require_role("manager")
    provider = providers.get(g.organization.id, provider_id) if provider_id else None
    if request.method == "POST":
        provider = providers.save(g.organization.id, current_user.id, request.form,
            request.form.getlist("capabilities"), provider_id, request.form.get("expected_version"))
        db.session.commit()
        flash("Provider saved.", "success")
        return redirect(url_for("uip_bp.provider_view", org_slug=org_slug, provider_id=provider.id))
    from app.models.uip import UipProviderCapability
    selected = {c.category for c in UipProviderCapability.query.filter_by(organization_id=g.organization.id, provider_id=provider_id)} if provider else set()
    return render_template("program_uip/providers/form.html", org=g.organization, provider=provider,
                           categories=sorted(CATEGORIES), selected=selected)


@uip_bp.route("/<org_slug>/providers/<int:provider_id>")
@login_required
def provider_view(org_slug, provider_id):
    _require_role("manager")
    from app.models.uip import UipProviderCapability, UipProviderUser
    provider = providers.get(g.organization.id, provider_id)
    links = db.session.query(UipProviderUser, User).join(CoreOrganizationMember,
        CoreOrganizationMember.id == UipProviderUser.membership_id).join(User, User.id == CoreOrganizationMember.user_id).filter(
        UipProviderUser.organization_id == g.organization.id, UipProviderUser.provider_id == provider_id,
        CoreOrganizationMember.organization_id == g.organization.id).all()
    members = db.session.query(CoreOrganizationMember, User).join(User, User.id == CoreOrganizationMember.user_id).filter(
        CoreOrganizationMember.organization_id == g.organization.id, CoreOrganizationMember.is_active.is_(True),
        User.id.in_([u.id for u in _members_with_roles("provider").all()])).all()
    capabilities = UipProviderCapability.query.filter_by(organization_id=g.organization.id, provider_id=provider_id).all()
    return render_template("program_uip/providers/view.html", org=g.organization, provider=provider, links=links, members=members, capabilities=capabilities)


@uip_bp.route("/<org_slug>/providers/<int:provider_id>/deactivate", methods=["POST"])
@login_required
def provider_deactivate(org_slug, provider_id):
    providers.deactivate(g.organization.id, current_user.id, provider_id, request.form.get("expected_version"))
    db.session.commit()
    flash("Provider deactivated.", "success")
    return redirect(url_for("uip_bp.provider_view", org_slug=org_slug, provider_id=provider_id))


@uip_bp.route("/<org_slug>/providers/<int:provider_id>/users", methods=["POST"])
@login_required
def provider_link(org_slug, provider_id):
    providers.associate(g.organization.id, current_user.id, provider_id,
        _positive_id(request.form.get("membership_id")), request.form.get("expected_version"))
    db.session.commit()
    flash("Provider association recorded. No role was granted.", "success")
    return redirect(url_for("uip_bp.provider_view", org_slug=org_slug, provider_id=provider_id))


@uip_bp.route("/<org_slug>/providers/<int:provider_id>/users/<int:link_id>/revoke", methods=["POST"])
@login_required
def provider_revoke(org_slug, provider_id, link_id):
    providers.revoke(g.organization.id, current_user.id, provider_id, link_id, request.form.get("expected_version"))
    db.session.commit()
    flash("Provider association revoked.", "success")
    return redirect(url_for("uip_bp.provider_view", org_slug=org_slug, provider_id=provider_id))


@uip_bp.route("/<org_slug>/work-orders")
@login_required
def work_order_list(org_slug):
    rows = work_orders.orders(g.organization.id, current_user.id).limit(200).all()
    return render_template("program_uip/work_orders/list.html", org=g.organization,
        orders=[work_orders.projection(g.organization.id, current_user.id, row.id) for row in rows])


@uip_bp.route("/<org_slug>/work-orders/<int:order_id>")
@login_required
def work_order_view(org_slug, order_id):
    from app.models.uip import UipWorkOrderAction
    order = work_orders.get(g.organization.id, current_user.id, order_id)
    staff_access = work_orders.is_staff(g.organization.id, current_user.id)
    allowed = []
    for action, (sources, destination, roles, reason_required) in work_orders.TRANSITIONS.items():
        if order.status not in sources:
            continue
        from werkzeug.exceptions import Forbidden
        try:
            audit.authorize(g.organization.id, current_user.id, roles)
        except Forbidden:
            continue
        if roles == ("provider",) and not providers.linked(g.organization.id, order.provider_id, current_user.id):
            continue
        if action in {"verified", "verification_rejected", "closed"} and providers.associated(g.organization.id, order.provider_id, current_user.id):
            continue
        allowed.append((action, reason_required))
    journal = UipWorkOrderAction.query.filter_by(organization_id=g.organization.id, work_order_id=order.id).order_by(UipWorkOrderAction.resulting_version).all()
    history = [dict(action=r.action, previous_state=r.previous_state, new_state=r.new_state,
                    occurred_at=r.occurred_at, actor_user_id=r.actor_user_id if staff_access else None,
                    version=r.resulting_version, reason_code=r.reason_code, dispatch_method=r.dispatch_method, note=r.note if staff_access or r.actor_user_id == current_user.id or r.action in {"verification_rejected", "cancelled"} else None)
               for r in journal if staff_access or r.action != "created"]
    return render_template("program_uip/work_orders/view.html" if staff_access else "program_uip/work_orders/provider_view.html",
        org=g.organization, order=work_orders.projection(g.organization.id, current_user.id, order_id),
        issue_reference=order.interaction.reference if staff_access else None, history=history,
        actions=allowed, request_key=str(uuid.uuid4()), reason_codes=sorted(audit.REASON_CODES),
        dispatch_methods=sorted(audit.DISPATCH_METHODS))


@uip_bp.route("/<org_slug>/work-orders/<int:order_id>/actions", methods=["POST"])
@login_required
def work_order_action(org_slug, order_id):
    order = work_orders.transition(g.organization.id, current_user.id, order_id, request.form.get("action"),
        request.form.get("expected_version"), request.form.get("request_key"), request.form.get("note"),
        request.form.get("reason_code"), request.form.get("dispatch_method"))
    db.session.commit()
    flash("Work-order action recorded.", "success")
    return redirect(url_for("uip_bp.work_order_view", org_slug=org_slug, order_id=order.id))
@uip_bp.route("/<org_slug>/service-status")
def service_status(org_slug):
    from flask import g, render_template
    # simple template
    return render_template("program_uip/service_status.html", org=g.organization)

@uip_bp.route("/<org_slug>/reset-genesis")
def reset_genesis(org_slug):
    """Temporary route to reset the UIP for Genesis testing."""
    from app.extensions import db
    from app.models.auth import User
    from app.models.uip import UipCommitteeMeeting, UipResolution
    from app.models.uip_governance import UipCommitteeTerm, UipCommitteeMember
    from app.models.core import CoreOrganizationMember, CoreInteraction
    from flask import flash, redirect, url_for, g

    org = g.organization

    try:
        # Wipe the governance records
        UipCommitteeMember.query.filter_by(organization_id=org.id).delete()
        UipResolution.query.filter_by(organization_id=org.id).delete()
        UipCommitteeTerm.query.filter_by(organization_id=org.id).delete()
        UipCommitteeMeeting.query.filter_by(organization_id=org.id).delete()
        
        # Wipe the test users
        users = User.query.filter(User.email.like('uip%')).all()
        if users:
            user_ids = [u.id for u in users]
            CoreOrganizationMember.query.filter(CoreOrganizationMember.user_id.in_(user_ids)).delete(synchronize_session=False)
            CoreInteraction.query.filter(CoreInteraction.creator_id.in_(user_ids)).delete(synchronize_session=False)
            User.query.filter(User.id.in_(user_ids)).delete(synchronize_session=False)
            
        db.session.commit()
        flash("Genesis Reset Complete! The UIP is now empty and test users have been purged.", "success")
        return redirect(url_for('auth_bp.logout'))
    except Exception as e:
        db.session.rollback()
        return f"Database Error during reset: {str(e)}"



@uip_bp.route("/<org_slug>/remove-trigger")
def remove_trigger(org_slug):
    """Temporary route to drop the immutable trigger."""
    from app.extensions import db
    from sqlalchemy import text
    try:
        db.session.execute(text("DROP FUNCTION IF EXISTS uip_p49_immutable CASCADE;"))
        db.session.commit()
        return "Successfully dropped uip_p49_immutable trigger function. You can now use /reset-genesis"
    except Exception as e:
        db.session.rollback()
        return f"Error: {e}"








@uip_bp.route("/<org_slug>/verify/unknown", methods=["GET"])
@login_required
def verify_unknown(org_slug):
    org = g.organization
    from app.models.core import CoreInteraction
    from app import db
    from flask_login import current_user
    
    claim = CoreInteraction.query.filter_by(
        organization_id=org.id,
        creator_id=current_user.id,
        interaction_type="unknown_claim",
        status="OPEN"
    ).first()
    if not claim:
        claim = CoreInteraction(
            organization_id=org.id,
            creator_id=current_user.id,
            interaction_type="unknown_claim",
            title="Unknown Role Claim",
            description=f"User {current_user.email} is unsure of their role and requests manual triage.",
            status="OPEN"
        )
        db.session.add(claim)
        db.session.commit()
    return redirect(url_for("uip_bp.my_access", org_slug=org.slug, claim="unknown_claim"))
@uip_bp.route("/<org_slug>/dev/upgrade-db")
def dev_upgrade_db(org_slug):
    from sqlalchemy import text
    try:
        db.session.execute(text("ALTER TABLE core_interaction ADD COLUMN parent_id INTEGER REFERENCES core_interaction(id);"))
        db.session.commit()
        return "Success"
    except Exception as e:
        db.session.rollback()
        return str(e)

@uip_bp.route("/<org_slug>/about")
def uip_about(org_slug):
    org = g.organization if hasattr(g, 'organization') else None
    return render_template("program_uip/about.html", org=org)
