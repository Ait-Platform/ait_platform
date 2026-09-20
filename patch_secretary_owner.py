with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# We need to completely replace _require_secretary
pattern = r'def _require_secretary\(\):.*?return current_appointment'

new_func = """def _require_secretary():
    org = g.organization
    
    # 1. Allow supreme system owner
    from app.models.core import CoreRoleAssignment, CoreRole
    owner_assignment = CoreRoleAssignment.query.filter(
        CoreRoleAssignment.organization_id == org.id,
        CoreRoleAssignment.user_id == current_user.id,
        CoreRoleAssignment.role.has(CoreRole.slug == 'owner')
    ).first()
    
    if owner_assignment:
        return None
        
    # 2. Check committee position
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not current_appointment or not current_appointment.position:
        abort(403, description="Access restricted.")
        
    pos = current_appointment.position.strip().lower()
    allowed = ["secretary", "chairperson", "chairman", "vice-chairperson", "vice chairman", "manager"]
    
    if pos not in allowed:
        abort(403, description="Access restricted to the active Secretary and Chairperson.")
        
    return current_appointment"""

text = re.sub(pattern, new_func, text, flags=re.DOTALL)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated require_secretary with owner bypass")
