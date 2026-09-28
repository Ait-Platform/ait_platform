import re
with open("app/program_uip/services/register.py", "r", encoding="utf-8") as f:
    content = f.read()

content = content.replace("def save_member(organization_id, actor_user_id, data, member_id=None, is_import=False, import_id=None):", "def save_member(organization_id, actor_user_id, data, member_id=None, is_import=False, import_id=None, is_authoritative=False):")
content = content.replace("def save_property(organization_id, actor_user_id, data, property_id=None, is_import=False, import_id=None):", "def save_property(organization_id, actor_user_id, data, property_id=None, is_import=False, import_id=None, is_authoritative=False):")
content = content.replace("def save_relationship(organization_id, actor_user_id, data, property_id=None, member_id=None, link_id=None, is_import=False, import_id=None):", "def save_relationship(organization_id, actor_user_id, data, property_id=None, member_id=None, link_id=None, is_import=False, import_id=None, is_authoritative=False):")

# Revert is_import=is_authoritative in process_import_batch back to is_import=True, is_authoritative=is_authoritative
content = content.replace("is_import=is_authoritative", "is_import=True, is_authoritative=is_authoritative")

with open("app/program_uip/services/register.py", "w", encoding="utf-8") as f:
    f.write(content)
print("Updated signatures and process_import_batch calls")
