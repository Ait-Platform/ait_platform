import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_term = """                        term = UipCommitteeTerm(
                            organization_id=org.id,
                            name="Genesis Term",
                            start_date=datetime.utcnow().date(),
                            status="ACTIVE"
                        )"""

new_term = """                        term = UipCommitteeTerm(
                            organization_id=org.id,
                            term_name="Genesis Term",
                            created_by=current_user.id
                        )"""

text = text.replace(old_term, new_term)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated term kwargs")
