import re
with open("app/program_uip/services/register.py", "r", encoding="utf-8") as f:
    content = f.read()

# Replace record_source="MUNICIPAL" if is_import else "MANUAL"
content = content.replace('record_source="MUNICIPAL" if is_import else "MANUAL"', 'record_source="MUNICIPAL" if is_authoritative else "MANUAL"')

# Replace if is_import: member.record_source = "MUNICIPAL"
content = content.replace("        if is_import:\n            member.record_source = \"MUNICIPAL\"", "        if is_authoritative:\n            member.record_source = \"MUNICIPAL\"")
content = content.replace("    if is_import:\n        if hasattr(item, \"record_source\"):\n            item.record_source = \"MUNICIPAL\"", "    if is_authoritative:\n        if hasattr(item, \"record_source\"):\n            item.record_source = \"MUNICIPAL\"")

with open("app/program_uip/services/register.py", "w", encoding="utf-8") as f:
    f.write(content)
print("Updated save_* functions to use is_authoritative")
