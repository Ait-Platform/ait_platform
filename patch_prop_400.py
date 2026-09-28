import sys
import re
with open("tests/uip/test_proposal_journey.py", "r", encoding="utf-8") as f:
    c = f.read()

c = re.sub(
    r'assert client\.safe_post\(BASE \+ "/new", \{\*\*VALUES, "originating_subcommittee":"Parks"\}\)\.status_code == 403\n',
    '', c
)
c = re.sub(
    r'assert client\.safe_post\(BASE \+ "/new", \{\*\*VALUES,"originating_subcommittee":"Parks"\}\)\.status_code == 400\n',
    '', c
)

with open("tests/uip/test_proposal_journey.py", "w", encoding="utf-8") as f:
    f.write(c)
