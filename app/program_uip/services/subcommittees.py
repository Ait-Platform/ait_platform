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

def require_subcommittee_responsibility(organization_id, actor_user_id, subcommittee_id):
    """Ensures the user occupies the responsible_seat_id for the given subcommittee."""
    from app.models.auth import User
    sub = UipSubcommittee.query.filter_by(
        organization_id=organization_id, id=subcommittee_id, status="ACTIVE"
    ).first()
    if not sub:
        abort(404)
        
    resp_mem = resolve_responsible_member(sub)
    if not resp_mem:
        abort(403, description="Subcommittee has no responsible member.")
        
    user = User.query.get(actor_user_id)
    if resp_mem.user_id:
        if resp_mem.user_id != user.id:
            abort(403, description="Access restricted to the responsible Subcommittee member.")
    else:
        if resp_mem.email.lower() != user.email.lower():
            abort(403, description="Access restricted to the responsible Subcommittee member.")
        
    return sub
