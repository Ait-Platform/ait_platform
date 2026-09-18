import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('@uip_bp.route("/<org_slug>/dev/upgrade-db")', '@uip_bp.route("/<org_slug>/apply-patch")')

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated route to /apply-patch")
