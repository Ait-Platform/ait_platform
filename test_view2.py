import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

start_idx = text.find("def dev_upgrade_db(org_slug):")
end_idx = text.find("@uip_bp", start_idx) if text.find("@uip_bp", start_idx) != -1 else start_idx + 2500
print(text[start_idx:end_idx])
