import re

filepath = 'app/program_uip/services/register.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search_code = '''        document_id=metadata.get("document_id"),
        status="PROCESSING"
    )'''

replace_code = '''        document_id=metadata.get("document_id"),
        notes=kind,
        status="PROCESSING"
    )'''

content = content.replace(search_code, replace_code)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated process_import_batch to store kind in notes")
