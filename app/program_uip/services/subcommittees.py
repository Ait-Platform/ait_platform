"""Subcommittee structural management and responsibility resolution."""
from flask import abort
from app.extensions import db
from app.models.uip import UipResolution
from app.models.uip_governance import UipSubcommittee, UipOrganogramSeat, UipCommitteeMember
from .reception import text

def resolve_responsible_member(subcommittee):
    """
    Returns the UipCommitteeMember who currently occupies the responsible_seat_id,
    or None if the seat is vacant or the occupant is not CURRENT.
    """
    if not subcommittee or not subcommittee.responsible_seat_id:
        return None
        
    return UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == subcommittee.organization_id,
        UipCommitteeMember.status == "CURRENT",
        UipCommitteeMember.seat_id == subcommittee.responsible_seat_id
    ).first()

def get_subcommittees(organization_id):
    """Return all ACTIVE subcommittees for an organization."""
    return UipSubcommittee.query.filter_by(
        organization_id=organization_id, status="ACTIVE"
    ).order_by(UipSubcommittee.name).all()

def create_subcommittee(organization_id, actor_user_id, name, resolution_id, responsible_seat_id, reports_to_seat_id):
    """Registers a new Subcommittee from an ADOPTED resolution."""
    # Authority check happens upstream (_require_secretary)
    
    clean_name = text(name, 255, True)
    
    # Must use a same-org ADOPTED resolution
    resolution = UipResolution.query.filter_by(
        organization_id=organization_id, id=resolution_id
    ).first()
    if not resolution or resolution.status != "ADOPTED":
        abort(400, description="A Subcommittee must be established by a same-organisation ADOPTED resolution.")
        
    # Seats must exist and belong to same org
    resp_seat = UipOrganogramSeat.query.filter_by(organization_id=organization_id, id=responsible_seat_id).first()
    report_seat = UipOrganogramSeat.query.filter_by(organization_id=organization_id, id=reports_to_seat_id).first()
    if not resp_seat or not report_seat:
        abort(400, description="Organogram seats must be valid and belong to this organisation.")
        
    # Duplicate active check
    existing = UipSubcommittee.query.filter_by(
        organization_id=organization_id, name=clean_name, status="ACTIVE"
    ).first()
    if existing:
        abort(400, description=f"An active subcommittee named '{clean_name}' already exists.")
        
    subcommittee = UipSubcommittee(
        organization_id=organization_id,
        name=clean_name,
        establishing_resolution_id=resolution.id,
        responsible_seat_id=resp_seat.id,
        reports_to_seat_id=report_seat.id,
        status="ACTIVE"
    )
    db.session.add(subcommittee)
    db.session.flush()
    return subcommittee


def current_memberships(organization_id):
    from datetime import date
    from sqlalchemy import or_
    from app.models.uip_governance import UipSubcommitteeMembership as Membership
    return Membership.query.join(UipCommitteeMember,
        (UipCommitteeMember.id == Membership.member_id) & (UipCommitteeMember.organization_id == Membership.organization_id)
    ).join(UipResolution, (UipResolution.id == Membership.appointing_resolution_id) & (UipResolution.organization_id == Membership.organization_id)
    ).join(UipSubcommittee, (UipSubcommittee.id == Membership.subcommittee_id) & (UipSubcommittee.organization_id == Membership.organization_id)).filter(
        Membership.organization_id == organization_id, Membership.status == "CURRENT",
        Membership.valid_from <= date.today(), or_(Membership.valid_to.is_(None), Membership.valid_to >= date.today()),
        UipCommitteeMember.status == "CURRENT", UipResolution.status == "ADOPTED", UipSubcommittee.status == "ACTIVE")


def memberships_for(organization_id, user):
    from sqlalchemy import and_, or_, func
    if not user or not user.is_active:
        return []
    identity = UipCommitteeMember.user_id == user.id
    if (user.email or "").strip():
        identity = or_(identity, and_(UipCommitteeMember.user_id.is_(None),
            func.lower(func.trim(UipCommitteeMember.email)) == user.email.strip().lower()))
    return current_memberships(organization_id).filter(identity).all()


def member_subcommittees(organization_id, user):
    ids = {m.subcommittee_id for m in memberships_for(organization_id, user)}
    return UipSubcommittee.query.filter(UipSubcommittee.organization_id == organization_id,
        UipSubcommittee.id.in_(ids)).order_by(UipSubcommittee.name).all() if ids else []


def require_subcommittee_membership(organization_id, actor_user_id, subcommittee_id):
    from app.models.auth import User
    sub = UipSubcommittee.query.filter_by(organization_id=organization_id, id=subcommittee_id, status="ACTIVE").first_or_404()
    user = db.session.get(User, actor_user_id)
    if not any(m.subcommittee_id == sub.id for m in memberships_for(organization_id, user)):
        abort(403, description="A current Resolution-backed membership of this Subcommittee is required.")
    return sub


def record_membership(org, actor, sub_id, member_id, resolution_id, valid_from, valid_to=None):
    # The Secretary records the appointment expressed by an adopted Resolution.
    from datetime import date
    from .proposals import require_official
    from app.models.uip_governance import UipSubcommitteeMembership as Membership
    if require_official(org, actor).position.strip().lower() != "secretary":
        abort(403)
    sub = UipSubcommittee.query.filter_by(organization_id=org, id=sub_id, status="ACTIVE").with_for_update().first_or_404()
    UipCommitteeMember.query.filter_by(organization_id=org, id=member_id, status="CURRENT").first_or_404()
    resolution = UipResolution.query.filter_by(organization_id=org, id=resolution_id, status="ADOPTED").first_or_404()
    try:
        start = date.fromisoformat(valid_from)
        end = date.fromisoformat(valid_to) if valid_to else None
        if end and end < start: raise ValueError()
    except (TypeError, ValueError):
        abort(400, description="Valid appointment dates are required.")
    row = Membership.query.filter_by(organization_id=org, subcommittee_id=sub.id, member_id=member_id,
        appointing_resolution_id=resolution.id).first()
    if row:
        if row.valid_from != start or row.valid_to != end:
            abort(409, description="This recorded appointment already has different dates.")
        return row
    row = Membership(organization_id=org, subcommittee_id=sub.id, member_id=member_id,
        appointing_resolution_id=resolution.id, valid_from=start, valid_to=end, recorded_by=actor)
    db.session.add(row)
    return row
