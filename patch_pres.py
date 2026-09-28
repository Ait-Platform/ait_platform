import re

filepath = 'app/program_uip/presentation.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Modify issue_rows to exclude claims and handle statuses
old_query = '''    for issue in CoreInteraction.query.filter_by(organization_id=org).order_by(
        CoreInteraction.created_at.desc().nullslast(), CoreInteraction.id.desc()).all():'''

new_query = '''    for issue in CoreInteraction.query.filter(
        CoreInteraction.organization_id == org,
        ~CoreInteraction.interaction_type.like('%_claim')
    ).order_by(CoreInteraction.created_at.desc().nullslast(), CoreInteraction.id.desc()).all():'''
content = content.replace(old_query, new_query)

old_status = '''        filters = {"all"}
        if issue.status == "RESOLVED":
            filters.add("resolved")
        else:
            filters.add("open")'''

new_status = '''        filters = {"all"}
        if issue.status in {"RESOLVED", "VERIFIED", "REJECTED", "DECLINED", "CLOSED"}:
            filters.add("resolved")
        else:
            filters.add("open")'''
content = content.replace(old_status, new_status)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated presentation.py")
