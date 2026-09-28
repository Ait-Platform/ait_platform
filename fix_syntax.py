import re

filepath = 'app/program_uip/operational_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if 'return page("Query #"' in line and '], rows, forms, notes)' in line:
        # Just replace the line entirely.
        lines[i] = '    return page("Query #" + str(issue.id) + (f" ({issue.reference})" if issue.reference else ""), ["Record", "Recorded time", "Method / department", "Outcome", "Next action / reference", "Due"], rows, forms, notes)\n'
        break

with open(filepath, 'w', encoding='utf-8') as f:
    f.writelines(lines)

print("Fixed SyntaxError")
