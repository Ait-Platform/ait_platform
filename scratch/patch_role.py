import re

with open('app/uip/__init__.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_logic = """                if org_slug == 'manor-gardens':
                    # Auto-enroll the user into the demo organization so they can explore it
                    membership = CoreOrganizationMember(
                        organization_id=org.id,
                        user_id=current_user.id,
                        is_active=True
                    )
                    db.session.add(membership)
                    db.session.commit()
                elif current_user.email != "sanjith@ait.com":"""

new_logic = """                if org_slug == 'manor-gardens':
                    # Auto-enroll the user into the demo organization so they can explore it
                    membership = CoreOrganizationMember(
                        organization_id=org.id,
                        user_id=current_user.id,
                        is_active=True
                    )
                    db.session.add(membership)
                    
                    from app.models.core import CoreRole, CoreRoleAssignment
                    manager_role = CoreRole.query.filter_by(slug='manager').first()
                    if not manager_role:
                        manager_role = CoreRole(name='Manager', slug='manager', access_level=10)
                        db.session.add(manager_role)
                        db.session.flush()
                        
                    assignment = CoreRoleAssignment.query.filter_by(user_id=current_user.id, organization_id=org.id).first()
                    if not assignment:
                        assignment = CoreRoleAssignment(
                            user_id=current_user.id,
                            organization_id=org.id,
                            role_id=manager_role.id
                        )
                        db.session.add(assignment)
                    
                    db.session.commit()
                elif current_user.email != "sanjith@ait.com":"""

text = text.replace(old_logic, new_logic)

with open('app/uip/__init__.py', 'w', encoding='utf-8') as f:
    f.write(text)
