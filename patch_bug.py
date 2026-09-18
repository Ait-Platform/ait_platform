import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_query = """                occupied = UipCommitteeMember.query.filter(
                    UipCommitteeMember.organization_id == org.id,
                    func.lower(UipCommitteeMember.position) == position.lower(),
                    UipCommitteeMember.is_active == True
                ).first()"""

new_query = """                occupied = UipCommitteeMember.query.filter(
                    UipCommitteeMember.organization_id == org.id,
                    func.lower(UipCommitteeMember.position) == position.lower(),
                    UipCommitteeMember.status == "CURRENT"
                ).first()"""

text = text.replace(old_query, new_query)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Fixed UipCommitteeMember.is_active bug")
