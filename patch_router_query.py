import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('["chairman", "vice chairman", "secretary", "treasurer"]', '["chairperson", "vice-chairperson", "secretary", "treasurer"]')

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated router_page query")
