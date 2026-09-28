import re
with open("app/program_uip/services/register.py", "r", encoding="utf-8") as f:
    content = f.read()

# Member protection
member_search = "existing = UipMemberProfile.query.filter_by(organization_id=organization_id, reference=reference).first()\n                if existing:"
member_replace = """existing = UipMemberProfile.query.filter_by(organization_id=organization_id, reference=reference).first()
                if existing:
                    if existing.record_source == "MUNICIPAL" and not is_authoritative:
                        raise Exception("Cannot overwrite authoritative municipal records.")"""
content = content.replace(member_search, member_replace)

# Property protection
prop_search = "existing = UipProperty.query.filter_by(organization_id=organization_id, reference=reference).first()\n                if existing:"
prop_replace = """existing = UipProperty.query.filter_by(organization_id=organization_id, reference=reference).first()
                if existing:
                    if existing.record_source == "MUNICIPAL" and not is_authoritative:
                        raise Exception("Cannot overwrite authoritative municipal records.")"""
content = content.replace(prop_search, prop_replace)

with open("app/program_uip/services/register.py", "w", encoding="utf-8") as f:
    f.write(content)
print("Added protection logic")
