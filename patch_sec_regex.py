import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

text = re.sub(r'elif pos == "secretary":\s*return redirect\(url_for\("uip_bp\.committee_dashboard", org_slug=org_slug\)\)', 'elif pos == "secretary":\n            return redirect(url_for("uip_bp.secretary_workspace", org_slug=org_slug))', text)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Regex replaced route")
