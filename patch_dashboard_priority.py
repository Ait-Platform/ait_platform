with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# We will modify the dashboard route to route Committee Members FIRST, even if they are owners.
pattern = r'def dashboard\(org_slug\):.*?# Normal routing'

new_start = """def dashboard(org_slug):
    org = g.organization
    
    # 1. Check if they are a Committee Member FIRST (so Chair/Treasurer go to their custom dashboards)
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if current_appointment:
        # Route them to the committee router, which handles Chair/Vice/Treasurer logic
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org_slug))

    # 2. Quick bypass for owners!
    from app.models.core import CoreRoleAssignment, CoreRole
    owner_assignment = CoreRoleAssignment.query.filter_by(
        organization_id=org.id, user_id=current_user.id
    ).join(CoreRole).filter(CoreRole.slug == 'owner').first()
    
    if owner_assignment:
        # If they are an owner (but not in committee), just give them the manager dashboard
        from app.program_uip.presentation import executive
        rows = []
        try:
            from app.program_uip.presentation import issue_rows
            rows = issue_rows(org.id, current_user.id)
        except Exception:
            pass
        return render_template("program_uip/dashboards/manager.html", org=org, overview={"cards": [], "issues": rows, "upcoming": 0, "clocks": []})

    # Normal routing"""

text = re.sub(pattern, new_start, text, flags=re.DOTALL)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated dashboard to prioritize committee routing")
