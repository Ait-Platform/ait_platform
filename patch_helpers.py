import re

def safe_auth_replacement(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        
    old_code = "audit.authorize(org, actor, providers.STAFF)"
    new_code = '''try:
        audit.authorize(org, actor, providers.STAFF)
    except Exception:
        from app.models.uip_governance import UipCommitteeMember
        from sqlalchemy import func, or_
        email_check = False
        from flask_login import current_user
        if current_user.email and current_user.email.strip():
            email_check = func.lower(func.trim(UipCommitteeMember.email)) == current_user.email.strip().lower()
        is_committee = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org,
            UipCommitteeMember.status == "CURRENT",
            or_(UipCommitteeMember.user_id == actor, email_check)
        ).first()
        if not is_committee:
            raise'''
            
    content = content.replace(old_code, new_code)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

safe_auth_replacement('app/program_uip/services/reception.py')
safe_auth_replacement('app/program_uip/services/routing.py')

print("Patched helper functions")
