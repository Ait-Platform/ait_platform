import re

with open('app/uip/__init__.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_logic = """            if not membership and current_user.email != "sanjith@ait.com":
                abort(403, description="Tenant Isolation Violation: You do not have access to this Organization.")"""

new_logic = """            if not membership:
                if org_slug == 'manor-gardens':
                    # Auto-enroll the user into the demo organization so they can explore it
                    membership = CoreOrganizationMember(
                        organization_id=org.id,
                        user_id=current_user.id,
                        is_active=True
                    )
                    db.session.add(membership)
                    db.session.commit()
                elif current_user.email != "sanjith@ait.com":
                    abort(403, description="Tenant Isolation Violation: You do not have access to this Organization.")"""

text = text.replace(old_logic, new_logic)

with open('app/uip/__init__.py', 'w', encoding='utf-8') as f:
    f.write(text)
