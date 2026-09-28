import sys
import re
with open("app/program_uip/services/proposals.py", "r", encoding="utf-8") as f:
    c = f.read()

# Replace originating_subcommittee checks
search_block = """    if values.get("originating_subcommittee"):
        abort(400, description="Subcommittee origin is not available yet.")"""

replace_block = """    sub_id = values.get("originating_subcommittee_id")
    if sub_id:
        from .subcommittees import require_subcommittee_responsibility
        sub = require_subcommittee_responsibility(org, actor, sub_id)
        row.originating_subcommittee_id = sub.id
    else:
        row.originating_subcommittee_id = None"""

c = c.replace(search_block, replace_block)

with open("app/program_uip/services/proposals.py", "w", encoding="utf-8") as f:
    f.write(c)
print("Updated proposals service")
