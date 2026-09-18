import re
with open("app/program_uip/completion_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_block = """    is_secretary = False
    try:
        sec = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == g.organization.id,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(current_user.email),
            UipCommitteeMember.position == "Secretary"
        ).first()
        is_secretary = bool(sec)
    except Exception:
        pass"""

new_block = """    is_secretary = False
    is_exco = False
    try:
        mem = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == g.organization.id,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
        ).first()
        if mem:
            is_exco = True
            if mem.position == "Secretary":
                is_secretary = True
    except Exception:
        pass"""

text = text.replace(old_block, new_block)

old_return = "uip_can_log=bool(roles & (staff | {\"committee_member\"})), is_secretary=is_secretary)"
new_return = "uip_can_log=bool(roles & (staff | {\"committee_member\"})), is_secretary=is_secretary, is_exco=is_exco)"
text = text.replace(old_return, new_return)

with open("app/program_uip/completion_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated context processor for is_exco")
