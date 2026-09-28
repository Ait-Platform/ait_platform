import re
with open("app/program_uip/services/register.py", "r", encoding="utf-8") as f:
    content = f.read()
# Find the first def require_register_admin, and strip all require_ blocks until available_memberships
start = content.find("def require_register_admin")
end = content.find("def available_memberships")
new_blocks = """def require_register_admin(organization_id, actor_user_id):
    from . import audit
    audit.authorize(organization_id, actor_user_id, ("manager", "RATEPAYER_ADMIN"))
    return True

def require_mo_vault_import(organization_id, actor_user_id):
    from . import audit
    audit.authorize(organization_id, actor_user_id, ("municipal_officer",))
    return True

def require_register_write(organization_id, actor_user_id):
    from . import audit
    audit.authorize(organization_id, actor_user_id, ("manager", "RATEPAYER_ADMIN", "municipal_officer"))
    return True

"""
new_content = content[:start] + new_blocks + content[end:]
with open("app/program_uip/services/register.py", "w", encoding="utf-8") as f:
    f.write(new_content)
print("Fixed!")
