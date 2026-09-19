with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "def router_page(org_slug):" in line:
        lines.insert(i + 1, "    from app.extensions import db\n")
        break

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.writelines(lines)
print("Patched router_page with top-level db import")
