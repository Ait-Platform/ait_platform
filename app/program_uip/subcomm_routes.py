"""Subcommittee Tools."""
from flask import g, render_template, request, redirect, url_for, abort
from flask_login import current_user, login_required
from app.models.uip_governance import UipSubcommittee
from . import uip_bp
from .services import subcommittees as sub_service

def _get_responsible_subcommittees(org_id, user):
    """Find all ACTIVE subcommittees where user is the responsible member."""
    subs = sub_service.get_subcommittees(org_id)
    my_subs = []
    for s in subs:
        resp = sub_service.resolve_responsible_member(s)
        if resp and resp.email.lower() == user.email.lower():
            my_subs.append(s)
    return my_subs

@uip_bp.route("/<org_slug>/sub-comm-tools")
@login_required
def sub_comm_tools_index(org_slug):
    org = g.organization
    my_subs = _get_responsible_subcommittees(org.id, current_user)
    
    if not my_subs:
        abort(403, description="You are not the responsible member for any active subcommittees.")
        
    if len(my_subs) == 1:
        return redirect(url_for("uip_bp.sub_comm_board", org_slug=org.slug, sub_id=my_subs[0].id))
        
    return render_template("program_uip/subcomm_tools/selector.html", org=org, subcommittees=my_subs)

@uip_bp.route("/<org_slug>/subcommittee/<int:sub_id>/board")
@login_required
def sub_comm_board(org_slug, sub_id):
    org = g.organization
    sub = sub_service.require_subcommittee_responsibility(org.id, current_user.id, sub_id)
    
    # We pass the resolution and seats for the 'My Subcommittee' tile view
    from app.models.uip import UipResolution
    from app.models.uip_governance import UipOrganogramSeat
    resolution = UipResolution.query.get(sub.establishing_resolution_id)
    resp_seat = UipOrganogramSeat.query.get(sub.responsible_seat_id)
    report_seat = UipOrganogramSeat.query.get(sub.reports_to_seat_id)
    resp_mem = sub_service.resolve_responsible_member(sub)
    
    return render_template("program_uip/subcomm_tools/board.html", 
        org=org, subcommittee=sub, resolution=resolution, 
        resp_seat=resp_seat, report_seat=report_seat, resp_mem=resp_mem)

# Tile Shells
@uip_bp.route("/<org_slug>/subcommittee/<int:sub_id>/members")
@login_required
def sub_comm_members(org_slug, sub_id):
    org = g.organization
    sub = sub_service.require_subcommittee_responsibility(org.id, current_user.id, sub_id)
    return render_template("program_uip/subcomm_tools/shell.html", org=org, subcommittee=sub, title="Members", message="No helper-membership persistence architecture exists yet.")

@uip_bp.route("/<org_slug>/subcommittee/<int:sub_id>/meetings")
@login_required
def sub_comm_meetings(org_slug, sub_id):
    org = g.organization
    sub = sub_service.require_subcommittee_responsibility(org.id, current_user.id, sub_id)
    return render_template("program_uip/subcomm_tools/shell.html", org=org, subcommittee=sub, title="Meetings & Agendas", message="Existing meeting architecture lacks subcommittee assignment relationship.")

@uip_bp.route("/<org_slug>/subcommittee/<int:sub_id>/mandates")
@login_required
def sub_comm_mandates(org_slug, sub_id):
    org = g.organization
    sub = sub_service.require_subcommittee_responsibility(org.id, current_user.id, sub_id)
    return render_template("program_uip/subcomm_tools/shell.html", org=org, subcommittee=sub, title="Mandates", message="No Mandate to Subcommittee assignment relationship exists yet.")

@uip_bp.route("/<org_slug>/subcommittee/<int:sub_id>/tasks")
@login_required
def sub_comm_tasks(org_slug, sub_id):
    org = g.organization
    sub = sub_service.require_subcommittee_responsibility(org.id, current_user.id, sub_id)
    return render_template("program_uip/subcomm_tools/shell.html", org=org, subcommittee=sub, title="Tasks & Actions", message="Existing task architecture lacks subcommittee assignment relationship.")

@uip_bp.route("/<org_slug>/subcommittee/<int:sub_id>/documents")
@login_required
def sub_comm_documents(org_slug, sub_id):
    org = g.organization
    sub = sub_service.require_subcommittee_responsibility(org.id, current_user.id, sub_id)
    return render_template("program_uip/subcomm_tools/shell.html", org=org, subcommittee=sub, title="Documents", message="Existing document architecture lacks subcommittee scope assignment.")

@uip_bp.route("/<org_slug>/subcommittee/<int:sub_id>/reports")
@login_required
def sub_comm_reports(org_slug, sub_id):
    org = g.organization
    sub = sub_service.require_subcommittee_responsibility(org.id, current_user.id, sub_id)
    return render_template("program_uip/subcomm_tools/shell.html", org=org, subcommittee=sub, title="Reports", message="Subcommittee-specific reporting workspace is empty.")

@uip_bp.route("/<org_slug>/subcommittee/<int:sub_id>/proposals", methods=["GET", "POST"])
@login_required
def sub_comm_proposals(org_slug, sub_id):
    org = g.organization
    sub = sub_service.require_subcommittee_responsibility(org.id, current_user.id, sub_id)
    
    # Render a proposal creation form scoping to this subcommittee
    from .services import proposals
    proposals_list = proposals.listing(org.id, current_user.id)
    # Filter only proposals matching this sub
    my_proposals = [p for p in proposals_list if getattr(p, 'originating_subcommittee_id', None) == sub.id]
    
    if request.method == "POST":
        values = request.form.to_dict()
        values["originating_subcommittee_id"] = sub.id
        doc_ids = request.form.getlist("document_id")
        try:
            row = proposals.save(org.id, current_user.id, values, doc_ids)
            from app.extensions import db
            db.session.commit()
            # Redirect to the proposal detail view in the regular shared proposal architecture
            return redirect(url_for("uip_bp.proposal_detail", org_slug=org.slug, proposal_id=row.id))
        except Exception as e:
            from app.extensions import db
            from flask import flash
            db.session.rollback()
            flash(str(e.description if hasattr(e, "description") else e), "danger")
            
    return render_template("program_uip/subcomm_tools/proposals.html", org=org, subcommittee=sub, proposals=my_proposals)
