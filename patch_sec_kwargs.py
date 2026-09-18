import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_sec = """                    new_sec = UipCommitteeMember(
                        organization_id=org.id,
                        user_id=current_user.id,
                        term_id=term.id,
                        name=current_user.name,
                        email=current_user.email,
                        level=level,
                        position=position,
                        status="CURRENT"
                    )"""

new_sec = """                    new_sec = UipCommitteeMember(
                        organization_id=org.id,
                        term_id=term.id,
                        name=current_user.name,
                        email=current_user.email,
                        position="Secretary",  # Hardcode to match DB constraint exactly
                        status="CURRENT",
                        created_by=current_user.id
                    )"""

text = text.replace(old_sec, new_sec)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated UipCommitteeMember kwargs")
