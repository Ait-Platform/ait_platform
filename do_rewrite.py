import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 1. Update completion_routes.py
import re
with open('app/program_uip/completion_routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Update CSV_COLUMNS
old_csv_cols = '''CSV_COLUMNS = {
    "members": ("reference", "name", "member_type", "email", "phone", "is_active", "eligibility_status"),
    "properties": ("reference", "address", "rates_reference", "classification", "is_active"),
    "relationships": ("member_reference", "property_reference", "relationship", "valid_from", "valid_to", "is_verified"),
}'''
new_csv_cols = '''CSV_COLUMNS = {
    "master_roll": ("member_reference", "name", "email", "id_number", "property_reference", "address", "classification")
}'''
content = content.replace(old_csv_cols, new_csv_cols)

# Update register_import method to just take master_roll
search_kind = 'kind = request.form.get("kind", "members")'
replace_kind = 'kind = "master_roll"'
content = content.replace(search_kind, replace_kind)

with open('app/program_uip/completion_routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

# 2. Update register.py (process_import_batch)
with open('app/program_uip/services/register.py', 'r', encoding='utf-8') as f:
    reg_content = f.read()

new_process = '''
def process_import_batch(organization_id, actor_user_id, kind, rows, metadata, is_authoritative=False):
    from app.models.uip import UipRegisterImport, UipRegisterImportException, UipPropertyMember, UipProperty, UipMemberProfile
    from app.extensions import db
    
    batch = UipRegisterImport(
        organization_id=organization_id,
        source_type="MUNICIPAL",
        source_identifier=metadata.get("source_identifier") or "MASTER_ROLL",
        batch_reference=metadata.get("batch_reference") or "MASTER",
        date_received=metadata.get("date_received"),
        effective_date=metadata.get("effective_date"),
        imported_by_user_id=actor_user_id,
        document_id=metadata.get("document_id"),
        notes="master_roll",
        status="PROCESSING"
    )
    db.session.add(batch)
    db.session.flush()
    
    summary = {"created": 0, "updated": 0, "exceptions": 0}
    
    for idx, row in enumerate(rows, 2):
        try:
            mem_ref = str(row.get("member_reference") or "").strip()
            prop_ref = str(row.get("property_reference") or "").strip()
            
            if not mem_ref or not prop_ref:
                raise Exception("Missing mandatory references")
                
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
            member.record_source = "MUNICIPAL"
            member.is_active = True
            
            # Raw Insert/Update Property
            prop = UipProperty.query.filter_by(organization_id=organization_id, reference=prop_ref).first()
            if not prop:
                prop = UipProperty(organization_id=organization_id, reference=prop_ref)
                db.session.add(prop)
            prop.address = (row.get("address") or "").strip()
            prop.classification = (row.get("classification") or "Residential").strip()
            prop.is_active = True
            
            db.session.flush()
            
            # Raw Insert/Update Link
            link = UipPropertyMember.query.filter_by(organization_id=organization_id, member_id=member.id, property_id=prop.id, valid_to=None).first()
            if not link:
                link = UipPropertyMember(organization_id=organization_id, member_id=member.id, property_id=prop.id)
                db.session.add(link)
                link.relationship = "owner"
                link.valid_from = batch.effective_date
                link.is_verified = True
            
        except Exception as e:
            db.session.add(UipRegisterImportException(
                import_id=batch.id,
                row_number=idx,
                source_reference=row.get("member_reference") or "unknown",
                reason=str(e),
                incoming_data=row
            ))
            summary["exceptions"] += 1
            
    batch.status = "COMPLETED" if summary["exceptions"] == 0 else "WITH_EXCEPTIONS"
    db.session.flush()
    return batch, summary
'''

reg_content = re.sub(r'def process_import_batch.*?return batch, summary\n', new_process, reg_content, flags=re.DOTALL)
with open('app/program_uip/services/register.py', 'w', encoding='utf-8') as f:
    f.write(reg_content)

# 3. Update router_page in routes.py for JIT
with open('app/program_uip/routes.py', 'r', encoding='utf-8') as f:
    routes_content = f.read()

jit_old = '''        member, properties, _ = vault_identity(org.id, current_user)
        if member:
            membership = CoreOrganizationMember(organization_id=org.id, user_id=current_user.id, is_active=True)
            from app.extensions import db
            db.session.add(membership)
            db.session.commit()'''
jit_new = '''        member, properties, _ = vault_identity(org.id, current_user)
        if member:
            membership = CoreOrganizationMember(organization_id=org.id, user_id=current_user.id, is_active=True)
            from app.extensions import db
            from app.models.uip import UipCommunicationPreference
            db.session.add(membership)
            
            # Also provision communication preference (Email) quietly
            pref = UipCommunicationPreference.query.filter_by(organization_id=org.id, member_id=member.id, channel="Email").first()
            if not pref:
                pref = UipCommunicationPreference(organization_id=org.id, member_id=member.id, channel="Email", preference="allowed")
                db.session.add(pref)
            db.session.commit()'''
routes_content = routes_content.replace(jit_old, jit_new)
with open('app/program_uip/routes.py', 'w', encoding='utf-8') as f:
    f.write(routes_content)

print("Backend rewritten!")
