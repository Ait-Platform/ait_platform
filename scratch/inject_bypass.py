import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

injection = """    banner = session.pop("payment_banner", None)
    
    # Inject CRM Receptionist Access
    if user_obj:
        try:
            from app.models.practice_crm import CrmPracticeUser
            is_crm_staff = CrmPracticeUser.query.filter_by(user_id=user_obj.id, status='active').first()
            if is_crm_staff and not any(getattr(r, 'slug', '') == 'practice_crm' for r in rows):
                from types import SimpleNamespace
                rows.append(SimpleNamespace(id=999, slug='practice_crm', name='Practice CRM', access_level='enrolled'))
        except Exception:
            pass
"""

content = content.replace('    banner = session.pop("payment_banner", None)', injection)

with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Injected CRM Staff bypass into bridge_dashboard")
