with open("app/program_uip/__init__.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

old_hook = """    if current_user.is_authenticated:
        membership = CoreOrganizationMember.query.filter_by(
            organization_id=org.id, user_id=current_user.id, is_active=True
        ).first()
        if not membership and request.endpoint not in public_endpoints:
            abort(403)"""

new_hook = """    if current_user.is_authenticated:
        membership = CoreOrganizationMember.query.filter_by(
            organization_id=org.id, user_id=current_user.id, is_active=True
        ).first()
        
        if not membership and request.endpoint not in public_endpoints:
            # Check if they have ANY explicit role assignment for this org (like owner, manager, etc)
            from app.models.core import CoreRoleAssignment
            has_role = CoreRoleAssignment.query.filter_by(
                organization_id=org.id, user_id=current_user.id
            ).first()
            
            if not has_role:
                abort(403, description="Access restricted. Active membership required.")"""

text = text.replace(old_hook, new_hook)

with open("app/program_uip/__init__.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated before_request membership check")
