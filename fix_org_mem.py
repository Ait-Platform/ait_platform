import re

filepath = 'app/program_uip/secretary_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search_code = '''    # 2. Grant the system role'''
replace_code = '''    from app.models.core import CoreOrganizationMember
    # 1.5 Ensure active organization membership
    org_mem = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=claim.creator_id).first()
    if not org_mem:
        org_mem = CoreOrganizationMember(organization_id=org.id, user_id=claim.creator_id, is_active=True)
        db.session.add(org_mem)
    else:
        org_mem.is_active = True

    # 2. Grant the system role'''

content = content.replace(search_code, replace_code)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Added org membership creation to verify_claim_via_mandate")
