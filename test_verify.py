import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

start_idx = text.find("def verify_committee(org_slug):")
end_idx = text.find("def verify_ratepayer", start_idx) if text.find("def verify_ratepayer", start_idx) != -1 else start_idx + 1500
print(text[start_idx:end_idx])
