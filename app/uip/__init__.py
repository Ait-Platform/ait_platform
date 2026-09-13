from flask import Blueprint, abort, g, request
from flask_login import current_user

uip_bp = Blueprint("uip_bp", __name__, url_prefix="/uip")


@uip_bp.before_request
def establish_organization_context():
    """Resolve an existing organisation without provisioning or schema changes."""
    if not request.view_args or "org_slug" not in request.view_args:
        return
    from app.models.core import CoreOrganization, CoreOrganizationMember

    org = CoreOrganization.query.filter_by(slug=request.view_args["org_slug"]).first()
    if not org:
        abort(404)
    g.organization = org
    g.org_id = org.id

    # Check commercial entitlement
    if request.endpoint != "uip_bp.service_status" and request.endpoint != "uip_bp.provisioning":
        from app.models.core import CoreOrganizationEntitlement
        from app.models.auth import AuthSubject
        uip_subject = AuthSubject.query.filter_by(slug='uip').first()
        if uip_subject:
            ent = CoreOrganizationEntitlement.query.filter_by(
                organization_id=org.id, subject_id=uip_subject.id
            ).first()
            if not ent or ent.status not in ("active", "complimentary"):
                from flask import redirect, url_for
                return redirect(url_for('uip_bp.service_status', org_slug=org.slug))
    
    # Allow public endpoints and the router to be accessed without membership
    public_endpoints = {
        "uip_bp.router_page", 
        "uip_bp.verify_public", 
        "uip_bp.public_dashboard",
        "uip_bp.verify_ratepayer",
        "uip_bp.verify_committee",
        "uip_bp.verify_mo",
        "uip_bp.verify_staff",
        "uip_bp.verify_subcommittee",
        "uip_bp.mo_dashboard",
        "uip_bp.subcommittee_dashboard",
        "uip_bp.waiting_lounge",
        "uip_bp.waiting_lounge_dispute",
        "uip_bp.service_status",
        "uip_bp.provisioning"
    }
    
    if current_user.is_authenticated:
        membership = CoreOrganizationMember.query.filter_by(
            organization_id=org.id, user_id=current_user.id, is_active=True
        ).first()
        if not membership and request.endpoint not in public_endpoints:
            abort(403)


from . import routes
from . import operational_routes
from . import completion_routes

from . import finance_routes

from . import pilot_routes


@uip_bp.record_once
def install_request_privacy(state):
    from .log_privacy import install
    install(state.app)
from . import provisioning_routes
from . import committee_routes
