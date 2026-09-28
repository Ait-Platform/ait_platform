import re

filepath = 'app/program_uip/routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

route_code = """
@uip_bp.route("/<org_slug>/verify/ratepayer/claim", methods=["POST"])
@login_required
def claim_ratepayer(org_slug):
    from app.models.core import CoreInteraction
    from app import db
    
    org = g.organization
    claim = CoreInteraction.query.filter_by(
        organization_id=org.id, creator_id=current_user.id, interaction_type="ratepayer_claim", status="OPEN"
    ).first()
    
    if not claim:
        claim = CoreInteraction(
            organization_id=org.id, creator_id=current_user.id,
            interaction_type="ratepayer_claim", title="Ratepayer Access Request",
            description=f"User {current_user.email} claims to be a ratepayer but the municipal vault is missing or unmatched.", 
            status="OPEN"
        )
        db.session.add(claim)
        db.session.commit()
        
    return redirect(url_for("uip_bp.my_access", org_slug=org_slug, claim="ratepayer"))
"""

if 'def claim_ratepayer' not in content:
    content += "\n" + route_code
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
print("Route added")
