"""Confined shared Proposal inbox and transition into existing Resolution drafts."""
from flask import g, render_template, request, redirect, url_for
from flask_login import current_user, login_required
from app.extensions import db
from app.models.uip import UipCommitteeMeeting, UipDocument, UipProposalDocument
from . import uip_bp
from .services import proposals as service, documents


@uip_bp.context_processor
def proposal_navigation():
    org = getattr(g, "organization", None)
    official = bool(org and current_user.is_authenticated and service.appointment(org.id, current_user.id))
    from app.models.uip import UipProposal
    pending = UipProposal.query.filter_by(organization_id=org.id, status="SUBMITTED").filter(UipProposal.originating_subcommittee_id.isnot(None)).count() if official else 0
    return {"can_use_proposals": official, "sub_comm_pending": pending}


@uip_bp.route("/<org_slug>/proposals")
@login_required
def proposal_list(org_slug):
    org = g.organization
    return render_template("program_uip/proposals/list.html", org=org,
        proposals=service.listing(org.id, current_user.id))


@uip_bp.route("/<org_slug>/proposals/new", methods=["GET", "POST"])
@uip_bp.route("/<org_slug>/proposals/<int:proposal_id>/edit", methods=["GET", "POST"])
@login_required
def proposal_edit(org_slug, proposal_id=None):
    org = g.organization
    if proposal_id is None and not request.form.get("originating_subcommittee_id"):
        service.require_official(org.id, current_user.id)
    if request.method == "POST":
        row = service.save(org.id, current_user.id, request.form, request.form.getlist("document_id"), proposal_id)
        db.session.commit()
        return redirect(url_for("uip_bp.proposal_detail", org_slug=org.slug, proposal_id=row.id))
    row = service.get(org.id, current_user.id, proposal_id) if proposal_id else None
    if row and not service.can_work_draft(org.id, current_user.id, row):
        from flask import abort
        abort(409)
    selected = {link.document_id for link in UipProposalDocument.query.filter_by(proposal_id=row.id, organization_id=org.id)} if row else set()
    return render_template("program_uip/proposals/edit.html", org=org, proposal=row,
        documents=service.available_documents(org.id, current_user.id), selected=selected)


@uip_bp.route("/<org_slug>/proposals/<int:proposal_id>")
@login_required
def proposal_detail(org_slug, proposal_id):
    org = g.organization
    row = service.get(org.id, current_user.id, proposal_id)
    linked = UipDocument.query.join(UipProposalDocument,
        UipProposalDocument.document_id == UipDocument.id).filter(
        UipProposalDocument.proposal_id == row.id,
        UipProposalDocument.organization_id == org.id,
        UipDocument.organization_id == org.id).all()
    visible = [doc for doc in linked if documents.accessible(org.id, current_user.id, doc)]
    meetings = UipCommitteeMeeting.query.filter_by(organization_id=org.id).order_by(UipCommitteeMeeting.id.desc()).all() if row.status == "SUBMITTED" else []
    return render_template("program_uip/proposals/detail.html", org=org, proposal=row,
        documents=visible, meetings=meetings, hidden_documents=len(linked)-len(visible),
        can_work_draft=service.can_work_draft(org.id, current_user.id, row),
        can_convert=bool(service.appointment(org.id, current_user.id)))


@uip_bp.route("/<org_slug>/proposals/<int:proposal_id>/submit", methods=["POST"])
@login_required
def proposal_submit(org_slug, proposal_id):
    org = g.organization
    row = service.submit(org.id, current_user.id, proposal_id)
    db.session.commit()
    return redirect(url_for("uip_bp.proposal_detail", org_slug=org.slug, proposal_id=row.id))


@uip_bp.route("/<org_slug>/proposals/<int:proposal_id>/convert", methods=["POST"])
@login_required
def proposal_convert(org_slug, proposal_id):
    org = g.organization
    row = service.convert(org.id, current_user.id, proposal_id, request.form.get("meeting_id"))
    db.session.commit()
    return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=row.resolution_id))


@uip_bp.route("/<org_slug>/sub-comm-control")
@login_required
def sub_comm_control(org_slug):
    org = g.organization
    service.require_official(org.id, current_user.id)
    from app.models.uip import UipProposal
    from app.models.uip_governance import UipSubcommittee
    rows = db.session.query(UipProposal, UipSubcommittee).join(UipSubcommittee,
        (UipSubcommittee.id == UipProposal.originating_subcommittee_id) &
        (UipSubcommittee.organization_id == UipProposal.organization_id)).filter(
        UipProposal.organization_id == org.id, UipProposal.status == "SUBMITTED").order_by(UipProposal.submitted_at).all()
    return render_template("program_uip/subcomm_tools/control.html", org=org, matters=rows)
