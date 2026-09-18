import re
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# I will append the secretary_intake route
new_route = """
@uip_bp.route("/<org_slug>/secretary-intake")
@login_required
def secretary_intake(org_slug):
    org = g.organization
    _require_secretary()
    
    # 1. Fetch pending claims from strangers/users
    open_claims = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.status == "OPEN",
        CoreInteraction.interaction_type.in_([
            "committee_claim", "ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim", "unknown_claim"
        ])
    ).order_by(CoreInteraction.created_at.asc()).all()
    
    # Enrich claims with user info
    enriched_claims = []
    for claim in open_claims:
        creator = User.query.get(claim.creator_id)
        enriched_claims.append({
            "id": claim.id,
            "type": claim.interaction_type,
            "title": claim.title,
            "description": claim.description,
            "created_at": claim.created_at,
            "user_name": creator.name or "User",
            "user_email": creator.email
        })
        
    return render_template(
        "program_uip/dashboards/secretary_intake.html",
        org=org,
        open_claims=enriched_claims
    )
"""

text += new_route

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added secretary_intake route")
