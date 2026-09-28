import sys
import re
with open("tests/uip/test_proposal_journey.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace("    assert client.safe_post(BASE + \"/123/convert\", {\"meeting_id\":\"1\"}).status_code == 403", "    assert client.safe_post(BASE + \"/123/convert\", {\"meeting_id\":\"1\"}).status_code == 403")
# Actually, I'll just remove extra spaces before it if any.

lines = []
for line in c.splitlines():
    if "assert client.safe_post(BASE + \"/123/convert\"" in line:
        line = "    assert client.safe_post(BASE + \"/123/convert\", {\"meeting_id\":\"1\"}).status_code == 403"
    lines.append(line)

with open("tests/uip/test_proposal_journey.py", "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
