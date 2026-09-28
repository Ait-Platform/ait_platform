import sys
with open("app/program_uip/services/subcommittees.py", "r", encoding="utf-8") as f:
    c = f.read()

guard_code = """
def require_subcommittee_responsibility(organization_id, actor_user_id, subcommittee_id):
    \"\"\"Ensures the user occupies the responsible_seat_id for the given subcommittee.\"\"\"
    from app.models.auth import User
    sub = UipSubcommittee.query.filter_by(
        organization_id=organization_id, id=subcommittee_id, status="ACTIVE"
    ).first()
    if not sub:
        abort(404)
        
    resp_mem = resolve_responsible_member(sub)
    if not resp_mem:
        abort(403, description="Subcommittee has no responsible member.")
        
    user = User.query.get(actor_user_id)
    if resp_mem.email.lower() != user.email.lower():
        abort(403, description="Access restricted to the responsible Subcommittee member.")
        
    return sub
"""

if "require_subcommittee_responsibility" not in c:
    c += guard_code

with open("app/program_uip/services/subcommittees.py", "w", encoding="utf-8") as f:
    f.write(c)
print("Updated subcommittees.py")
