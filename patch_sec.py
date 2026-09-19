with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """                    pos = "Secretary" if claim.interaction_type == "secretary_claim" else (claim.title.split(": ")[-1] if ":" in claim.title else "Unassigned")"""

new_logic = """                    pos = "Secretary" if claim.interaction_type == "secretary_claim" else (claim.title.split(": ")[-1] if ":" in claim.title else (claim.title.split(" - ")[-1] if " - " in claim.title else "Unassigned"))"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched secretary_routes.py")
