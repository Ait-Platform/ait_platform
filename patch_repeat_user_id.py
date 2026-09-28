import sys
import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    c = f.read()

c = re.sub(r'user_id=claim\.creator\.id,\n\s*user_id=claim\.creator\.id,\n', 'user_id=claim.creator.id,\n', c)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(c)
