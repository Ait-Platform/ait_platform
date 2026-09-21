with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    sec_routes = f.read()

import re
# Extract the logic from secretary_workspace to fetch claims and stats
pattern = r'def secretary_workspace\(org_slug\):.*?# 1\. Fetch pending claims(.*?)(?:return render_template)'
match = re.search(pattern, sec_routes, flags=re.DOTALL)
logic = match.group(1) if match else ""

# Now, we update committee_routes.py to put this logic inside chairman_workspace
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    comm_routes = f.read()

# Replace the current chairman_workspace function
old_chairman = """@uip_bp.route("/<org_slug>/chairman-workspace")
@login_required
def chairman_workspace(org_slug):
    org = g.organization
    return render_template("program_uip/dashboards/placeholder_workspace.html", org=org, role_title="Chairman", role_desc="Oversee the entire precinct.")"""

# We need to make sure we import User and CoreInteraction if not already imported in this scope, but they usually are globally imported at the top. Let's just do safe imports inside the function.
new_chairman = """@uip_bp.route("/<org_slug>/chairman-workspace")
@login_required
def chairman_workspace(org_slug):
    org = g.organization
    
    from app.models.core import CoreInteraction
    from app.models.auth import User
    from app.models.uip import UipResolution
    from app.models.uip_governance import UipCommitteeTerm, UipCommitteeMember
    
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
            "user_name": creator.name if creator else "Unknown",
            "user_email": creator.email if creator else "Unknown",
        })
        
    # 2. Gate Status Flags
    switch_gate = 'red' if enriched_claims else 'clear'
    
    # 3. Resolutions logic
    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    proposed_res = UipResolution.query.filter_by(organization_id=org.id, status="DRAFT").count()
    
    if tabled_res > 0:
        switch_res = 'red'
    elif proposed_res > 0:
        switch_res = 'amber'
    else:
        switch_res = 'clear'
        
    return render_template(
        "program_uip/dashboards/chairman_workspace.html",
        org=org,
        open_claims=enriched_claims,
        switch_gate=switch_gate,
        switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=proposed_res
    )"""

comm_routes = comm_routes.replace(old_chairman, new_chairman)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(comm_routes)

print("Updated chairman_workspace route logic to match secretary_workspace")
