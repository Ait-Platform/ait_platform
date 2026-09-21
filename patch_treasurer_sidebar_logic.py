with open("app/program_uip/completion_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re
old_logic = """is_secretary = False
    is_chairman = False
    is_exco = False
    try:
        mem = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == g.organization.id,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
        ).first()
        if mem:
            is_exco = True
            pos = mem.position.lower() if mem.position else ""
            if pos in ["secretary", "treasurer"]:  # We will keep treasurer on secretary tools for now unless specified
                is_secretary = True
            elif pos in ["chairperson", "chairman", "vice-chairperson", "vice chairman"]:
                is_chairman = True
    except Exception:
        pass"""

new_logic = """is_secretary = False
    is_chairman = False
    is_treasurer = False
    is_exco = False
    try:
        mem = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == g.organization.id,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
        ).first()
        if mem:
            is_exco = True
            pos = mem.position.lower() if mem.position else ""
            if pos == "secretary":
                is_secretary = True
            elif pos == "treasurer":
                is_treasurer = True
            elif pos in ["chairperson", "chairman", "vice-chairperson", "vice chairman"]:
                is_chairman = True
    except Exception:
        pass"""

text = text.replace(old_logic, new_logic)
text = text.replace('is_secretary=is_secretary, is_chairman=is_chairman, is_exco=is_exco)', 'is_secretary=is_secretary, is_chairman=is_chairman, is_treasurer=is_treasurer, is_exco=is_exco)')

with open("app/program_uip/completion_routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated completion routes for treasurer")
