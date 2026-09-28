import re

filepath = 'app/program_uip/routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search_router = '''    # 1b. Auto-route if active Ratepayer
    from app.models.core import CoreOrganizationMember
    membership = CoreOrganizationMember.query.filter_by(
        organization_id=org.id, user_id=current_user.id, is_active=True
    ).first()
    
    if membership and not force_menu:
        # Since they don't have a committee appointment, but they are an active member,
        # they are a Ratepayer (or other standard role).
        return redirect(url_for("uip_bp.dashboard", org_slug=org.slug))'''

replace_router = '''    # 1b. Auto-route if active Ratepayer
    from app.models.core import CoreOrganizationMember
    membership = CoreOrganizationMember.query.filter_by(
        organization_id=org.id, user_id=current_user.id, is_active=True
    ).first()
    
    # 1c. Vault Auto-Provisioning for Strangers
    if not membership:
        from .services.ratepayer import vault_identity
        member, properties, _ = vault_identity(org.id, current_user)
        if member:
            membership = CoreOrganizationMember(organization_id=org.id, user_id=current_user.id, is_active=True)
            from app.extensions import db
            db.session.add(membership)
            db.session.commit()
    
    if membership and not force_menu:
        # Since they don't have a committee appointment, but they are an active member,
        # they are a Ratepayer (or other standard role).
        return redirect(url_for("uip_bp.dashboard", org_slug=org.slug))'''

content = content.replace(search_router, replace_router)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Prepared vault auto-provisioning logic")
