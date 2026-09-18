import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Remove the faulty role check
text = text.replace('    _require_role("secretary") # Assuming secretary is required, but let\'s use the DB check\n', '')

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Removed _require_role")
