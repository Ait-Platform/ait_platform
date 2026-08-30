from flask import Blueprint, g, request, abort
from app.extensions import db

uip_bp = Blueprint("uip_bp", __name__, url_prefix="/uip")

@uip_bp.before_request
def establish_organization_context():
    """
    Middleware to automatically extract org_slug from the URL and
    inject the Organization into the global g context.
    Enforces tenant isolation globally for all UIP routes.
    """
    if request.view_args and 'org_slug' in request.view_args:
        org_slug = request.view_args['org_slug']
        
        # Import inside the function to avoid circular imports
        from app.models.core import CoreOrganization
        
        org = CoreOrganization.query.filter_by(slug=org_slug).first()
        if not org:
            abort(404, description=f"Organisation '{org_slug}' not found.")
            
        g.organization = org

from . import routes
