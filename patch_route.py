import re

filepath = 'app/program_uip/completion_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# I need to add flask.session support. Let's find the route definition and replace it.
# Actually, I can just write a script to replace the entire register_import function block.

search_block = '''def register_import(org_slug):
    from app.program_uip.services.register import require_register_admin, require_mo_vault_import, process_import_batch'''

replace_block = '''def register_import(org_slug):
    from flask import session
    from app.program_uip.services.register import require_register_admin, require_mo_vault_import, process_import_batch'''

content = content.replace(search_block, replace_block)
with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
