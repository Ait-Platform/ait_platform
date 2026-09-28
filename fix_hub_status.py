import re

filepath = 'app/program_uip/completion_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the query to include WITH_EXCEPTIONS
search_query = '''imports = UipRegisterImport.query.filter_by(organization_id=g.organization.id, batch_reference=batch_ref, status="COMPLETED").all()'''
replace_query = '''imports = UipRegisterImport.query.filter(
            UipRegisterImport.organization_id == g.organization.id,
            UipRegisterImport.batch_reference == batch_ref,
            UipRegisterImport.status.in_(["COMPLETED", "WITH_EXCEPTIONS"])
        ).all()'''

content = content.replace(search_query, replace_query)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated route to accept WITH_EXCEPTIONS as completed step")
