import re

filepath = 'app/program_uip/operational_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update Title to Queries Desk
old_title_reception = '''    return page("Central Inbox Queries Desk", ["Reference", "Logged", "Member", "Property", "Fault type", "Status"],
        [(link(i.reference or "Draft", "reception_issue", issue_id=i.id),'''
new_title_reception = '''    return page("Queries Desk", ["Reference", "Logged", "Member", "Property", "Fault type", "Status"],
        [(link(i.reference or "Draft", "reception_issue", issue_id=i.id),'''
content = content.replace(old_title_reception, new_title_reception)

# 2. Update the "Reception —" return to "Query #" and add the Back Button to 
otes
# Wait, let's find the exact return statement for eception_issue.
old_return_issue = '''    if issue.reference:
        notes.append(link("Issue and work orders", "view_interaction", reference=issue.reference))
    return page("Reception — " + (issue.reference or str(issue.id)), ["Record", "Recorded time", "Method / department", "Outcome", "Next action / reference", "Due"], rows, forms, notes)'''

new_return_issue = '''    if issue.reference:
        notes.append(link("Issue and work orders", "view_interaction", reference=issue.reference))
    notes.insert(0, link("← Back to Queries Desk", "reception_page"))
    return page("Query #" + str(issue.id) + (f" ({issue.reference})" if issue.reference else ""), ["Record", "Recorded time", "Method / department", "Outcome", "Next action / reference", "Due"], rows, forms, notes)'''
content = content.replace(old_return_issue, new_return_issue)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated operational_routes.py for titles and back button")
