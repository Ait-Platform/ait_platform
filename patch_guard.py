import sys
with open("app/program_uip/services/subcommittees.py", "r", encoding="utf-8") as f:
    c = f.read()

guard_code = """    user = User.query.get(actor_user_id)
    if resp_mem.email.lower() != user.email.lower():
        abort(403, description="Access restricted to the responsible Subcommittee member.")"""

new_guard = """    user = User.query.get(actor_user_id)
    if resp_mem.user_id:
        if resp_mem.user_id != user.id:
            abort(403, description="Access restricted to the responsible Subcommittee member.")
    else:
        if resp_mem.email.lower() != user.email.lower():
            abort(403, description="Access restricted to the responsible Subcommittee member.")"""

c = c.replace(guard_code, new_guard)

with open("app/program_uip/services/subcommittees.py", "w", encoding="utf-8") as f:
    f.write(c)
