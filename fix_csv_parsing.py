filepath = 'app/program_uip/services/register.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

new_process = '''
    for idx, raw_row in enumerate(rows, 2):
        try:
            # Strip whitespace from dictionary keys to forgive messy CSV headers
            row = {k.strip(): v for k, v in raw_row.items() if k is not None}
            
            mem_ref = str(row.get("member_reference") or "").strip()
            prop_ref = str(row.get("property_reference") or "").strip()
            
            if not mem_ref or not prop_ref:
                raise Exception("Missing mandatory references (member_reference or property_reference)")
                
            # Raw Insert/Update Member
            member = UipMemberProfile.query.filter_by(organization_id=organization_id, reference=mem_ref).first()
            if not member:
                member = UipMemberProfile(organization_id=organization_id, reference=mem_ref)
                db.session.add(member)
                summary["created"] += 1
            else:
                summary["updated"] += 1
                
            member.name = (row.get("name") or "").strip()
            member.email = (row.get("email") or "").strip()
            member.phone = (row.get("phone") or "").strip()
            member.member_type = (row.get("member_type") or "person").strip().lower()
            member.record_source = "MUNICIPAL"
            member.is_active = True
            member.last_import_id = batch.id
            
            # Raw Insert/Update Property
            prop = UipProperty.query.filter_by(organization_id=organization_id, reference=prop_ref).first()
            if not prop:
                prop = UipProperty(organization_id=organization_id, reference=prop_ref)
                db.session.add(prop)
            prop.address = (row.get("address") or "").strip()
            prop.rates_reference = (row.get("rates_reference") or "").strip()
            prop.classification = (row.get("classification") or "Residential").strip()
            prop.is_active = True
            prop.last_import_id = batch.id
'''

import re
content = re.sub(r'    for idx, row in enumerate\(rows, 2\):\s+try:\s+mem_ref = .*?prop\.last_import_id = batch\.id', new_process, content, flags=re.DOTALL)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated register.py to forgive spaces in headers and capture phone/rates")
