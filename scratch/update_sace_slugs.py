import re

# 1. Update provisioning_map.html
file1 = 'templates/program_sace/provisioning_map.html'
with open(file1, 'r', encoding='utf-8') as f:
    text = f.read()
text = text.replace("subject='sace'", "subject='sace_endorsement'")
with open(file1, 'w', encoding='utf-8') as f:
    f.write(text)

# 2. Update app/program_sace/routes.py (auditor_pledge redirect)
file2 = 'app/program_sace/routes.py'
with open(file2, 'r', encoding='utf-8') as f:
    text = f.read()
text = text.replace("subject='sace'", "subject='sace_endorsement'")
with open(file2, 'w', encoding='utf-8') as f:
    f.write(text)

# 3. Update auth/routes.py to handle sace_endorsement natively instead of sace
file3 = 'app/auth/routes.py'
with open(file3, 'r', encoding='utf-8') as f:
    text = f.read()

# Update inference logic
text = text.replace('if "/sace" in n_url_lower:', 'if "/sace" in n_url_lower:\n            return "sace_endorsement"')
text = text.replace('return "sace"', '') # clean up the old one

# Update register_decision conditions
text = text.replace('"cptd", "sace", "uip"', '"cptd", "sace_endorsement", "sace", "uip"')
text = text.replace('elif subject == "sace":', 'elif subject in ("sace", "sace_endorsement"):')

with open(file3, 'w', encoding='utf-8') as f:
    f.write(text)

print("Updated sace to sace_endorsement in templates and routes.")
