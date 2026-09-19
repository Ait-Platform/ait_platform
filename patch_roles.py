with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """                  port = portfolio_map.get(str(claim.id)) or portfolio_map.get(claim.id) or claim.interaction_type.replace('_claim', '').title()"""

new_logic = """                  # Extract the specific position they requested from the claim title (e.g. "Committee Claim: Chairperson")
                  requested_pos = claim.title.split(": ")[-1] if ":" in claim.title else claim.interaction_type.replace('_claim', '').title()
                  port = portfolio_map.get(str(claim.id)) or portfolio_map.get(claim.id) or requested_pos"""

if old_logic in text:
    text = text.replace(old_logic, new_logic)
    print("Patched committee_routes.py")
else:
    print("Could not find old logic in committee_routes.py")

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)

with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_sec = """                    pos = "Secretary" if claim.interaction_type == "secretary_claim" else "Unassigned\""""
new_sec = """                    pos = "Secretary" if claim.interaction_type == "secretary_claim" else (claim.title.split(": ")[-1] if ":" in claim.title else "Unassigned")"""

if old_sec in text:
    text = text.replace(old_sec, new_sec)
    print("Patched secretary_routes.py")
else:
    print("Could not find old logic in secretary_routes.py")

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
