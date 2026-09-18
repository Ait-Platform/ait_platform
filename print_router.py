import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

start_idx = text.find("def router_page(org_slug):")
end_idx = text.find("def my_access(org_slug):", start_idx)
print(text[start_idx:end_idx])
