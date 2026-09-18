import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

start_idx = text.find("def verify_committee(org_slug):")
end_idx = text.find("@uip_bp.route", start_idx + 10)
print(text[start_idx:end_idx])
