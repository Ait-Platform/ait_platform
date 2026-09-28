import sys
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace("""                    user_id=claim.creator.id,
                    name=claim.creator.name,
                    user_id=claim.creator.id,
                        email=claim.creator.email,""", """                    user_id=claim.creator.id,
                    name=claim.creator.name,
                    email=claim.creator.email,""")

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(c)
