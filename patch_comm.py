with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """            if 'committee' in claim.interaction_type:
                requested_pos = claim.title.split(": ")[-1] if ":" in claim.title else claim.interaction_type.replace('_claim', '').title()"""

new_logic = """            if 'committee' in claim.interaction_type:
                requested_pos = claim.title.split(": ")[-1] if ":" in claim.title else (claim.title.split(" - ")[-1] if " - " in claim.title else claim.interaction_type.replace('_claim', '').title())"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched committee_routes.py")
