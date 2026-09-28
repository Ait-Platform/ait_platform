import re
with open("app/program_uip/services/register.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace(
    'is_manager = CoreRoleAssignment.query.join(CoreRole).filter(',
    'print(f"DEBUG: org={organization_id} user={actor_user_id}"); is_manager = CoreRoleAssignment.query.join(CoreRole).filter('
)
text = text.replace(
    'abort(403, description="Register administration authority required.")',
    'print("DEBUG: ABORTING"); abort(403, description="Register administration authority required.")'
)

with open("app/program_uip/services/register.py", "w", encoding="utf-8") as f:
    f.write(text)
