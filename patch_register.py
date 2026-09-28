import re
with open("app/program_uip/services/register.py", "r", encoding="utf-8") as f:
    content = f.read()

# Replace signature
content = content.replace("def process_import_batch(organization_id, actor_user_id, kind, rows, metadata):", "def process_import_batch(organization_id, actor_user_id, kind, rows, metadata, is_authoritative=False):")

# Replace is_import=True with is_import=is_authoritative
import_func_body_start = content.find("def process_import_batch")
import_func_body = content[import_func_body_start:]
import_func_body = import_func_body.replace("is_import=True", "is_import=is_authoritative")
content = content[:import_func_body_start] + import_func_body

with open("app/program_uip/services/register.py", "w", encoding="utf-8") as f:
    f.write(content)
print("Updated process_import_batch signature and is_import flags")
