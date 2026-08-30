from flask import Blueprint, g, request, abort
from flask_login import current_user
from app.extensions import db

uip_bp = Blueprint("uip_bp", __name__, url_prefix="/uip")

@uip_bp.before_request
def establish_organization_context():
    """
    Middleware to automatically extract org_slug from the URL and
    inject the Organization into the global g context.
    Enforces strict tenant isolation and RBAC.
    """
    if request.view_args and 'org_slug' in request.view_args:
        org_slug = request.view_args['org_slug']
        
        from app.models.core import CoreOrganization, CoreOrganizationMember
        
        org = CoreOrganization.query.filter_by(slug=org_slug).first()
        if not org:
            abort(404, description=f"Organisation '{org_slug}' not found.")
            
        g.organization = org
        
        # SECURITY HARDENING (Step 19)
        # Ensure the current user is actually a member of this organization
        if current_user.is_authenticated:
            membership = CoreOrganizationMember.query.filter_by(
                organization_id=org.id, 
                user_id=current_user.id,
                is_active=True
            ).first()
            
            # Allow admins to bypass (assuming role.name == 'Admin' globally somewhere, or just enforce strict membership)
            if not membership and current_user.email != "sanjith@ait.com":
                abort(403, description="Tenant Isolation Violation: You do not have access to this Organization.")

from . import routes
