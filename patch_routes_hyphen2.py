with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic2 = """              if claim and ":" in claim.title:
                  new_pos = claim.title.split(": ")[-1]
                  appointment.position = new_pos
                  db.session.commit()
                  pos = new_pos.strip().lower()"""

new_logic2 = """              if claim:
                  new_pos = claim.title.split(": ")[-1] if ":" in claim.title else claim.title.split(" - ")[-1]
                  appointment.position = new_pos.strip()
                  db.session.commit()
                  pos = new_pos.strip().lower()"""

text = text.replace(old_logic2, new_logic2)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched dashboard auto fix in routes.py")
