import re
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_check = "if 'committee' in claim.interaction_type:"
new_check = "if claim.interaction_type in ['committee_claim', 'secretary_claim']:"

text = text.replace(old_check, new_check)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched creation of UipCommitteeMember")
