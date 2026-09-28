import re

filepath = 'app/program_uip/operational_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

old_title_reception = 'return page("Central Inbox Queries Desk", ["Reference", "Logged", "Member", "Property", "Fault type", "Status"],'
new_title_reception = 'return page("Queries Desk", ["Reference", "Logged", "Member", "Property", "Fault type", "Status"],'
content = content.replace(old_title_reception, new_title_reception)

old_return_issue = 'return page("Reception - " + (issue.reference or str(issue.id)), ["Record", "Recorded time", "Method / department", "Outcome", "Next action / reference", "Due"], rows, forms, notes)'
new_return_issue = '''notes.insert(0, link("<- Back to Queries Desk", "reception_page"))
    return page("Query #" + str(issue.id) + (f" ({issue.reference})" if issue.reference else ""), ["Record", "Recorded time", "Method / department", "Outcome", "Next action / reference", "Due"], rows, forms, notes)'''
# Note: The original had the em dash "?". Let's just use regex to replace it to be safe.
content = re.sub(r'return page\("Reception[^\+]*\+\s*\(issue\.reference or str\(issue\.id\)\), \["Record"', new_return_issue.replace('<-', '&larr;'), content)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated ui titles")
