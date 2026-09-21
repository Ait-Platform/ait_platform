with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# 1. Update the normal routing in dashboard()
old_routing = """if role_slug in {"resident", "owner"}:
        return redirect(url_for("uip_bp.my_access", org_slug=org_slug))"""

new_routing = """if role_slug in {"resident", "owner"}:
        return redirect(url_for("uip_bp.ratepayer_workspace", org_slug=org_slug))"""

text = text.replace(old_routing, new_routing)

# 2. Add the ratepayer_workspace route
ratepayer_route = """
@uip_bp.route("/<org_slug>/ratepayer-workspace")
@login_required
def ratepayer_workspace(org_slug):
    org = g.organization
    
    # 1. Fetch public resolutions
    from app.models.uip import UipResolution
    public_resolutions = UipResolution.query.filter_by(
        organization_id=org.id, voting_scope='PUBLIC'
    ).order_by(UipResolution.created_at.desc()).all()
    
    # 2. Fetch user's interactions (faults/service requests)
    from app.models.core import CoreInteraction
    my_requests = CoreInteraction.query.filter_by(
        organization_id=org.id, creator_id=current_user.id
    ).filter(CoreInteraction.interaction_type == 'municipal_fault').order_by(CoreInteraction.created_at.desc()).all()
    
    # 3. Fetch user's verified properties
    from app.models.uip_governance import UipPropertyMember, UipProperty
    my_properties = db.session.query(UipProperty).join(UipPropertyMember).filter(
        UipPropertyMember.organization_id == org.id,
        UipPropertyMember.user_id == current_user.id
    ).all()
    
    return render_template(
        "program_uip/dashboards/ratepayer_workspace.html",
        org=org,
        public_resolutions=public_resolutions,
        my_requests=my_requests,
        my_properties=my_properties
    )

"""

if "def ratepayer_workspace" not in text:
    # Append to the end of the file or before the bottom
    text = text + ratepayer_route

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Added ratepayer_workspace route and updated dashboard routing")
