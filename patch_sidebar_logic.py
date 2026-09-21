with open("app/program_uip/completion_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# We need to add is_chairman to the context variables
# Find the context processor logic
pattern = r'is_secretary = False.*?try:.*?if mem:.*?is_exco = True.*?if mem\.position and mem\.position\.lower\(\) in \["secretary", "chairperson", "chairman", "vice-chairperson", "vice chairman", "treasurer"\]:.*?is_secretary = True.*?except Exception:.*?pass'

# We will replace it with a cleaner roles evaluation
new_logic = """is_secretary = False
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

text = re.sub(pattern, new_logic, text, flags=re.DOTALL)

# Remove "Command Centre" from the sections tuple
text = text.replace('("Command Centre", [("Overview", "dashboard")]),', '')

# Ensure we pass is_chairman to the dictionary
text = text.replace('is_secretary=is_secretary, is_exco=is_exco)', 'is_secretary=is_secretary, is_chairman=is_chairman, is_exco=is_exco)')

with open("app/program_uip/completion_routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated completion routes with is_chairman and removed Command Centre")
