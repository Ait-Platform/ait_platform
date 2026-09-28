import re

filepath = 'app/program_uip/committee_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Update decide_resolution to handle description
description_logic = '''        # 1. Capture meeting details
        meeting_date_str = request.form.get("meeting_date")
        meeting_location = request.form.get("meeting_location")
        
        # Update mandate content if provided
        updated_description = request.form.get("description")
        if updated_description:
            res.description = updated_description
            
        live_yea = request.form.get("live_yea", type=int, default=0)
        live_nay = request.form.get("live_nay", type=int, default=0)
        live_abstain = request.form.get("live_abstain", type=int, default=0)'''

content = re.sub(
    r'# 1\. Capture meeting details\s*meeting_date_str = request\.form\.get\("meeting_date"\)\s*meeting_location = request\.form\.get\("meeting_location"\)\s*live_yea = request\.form\.get\("live_yea", type=int, default=0\)\s*live_nay = request\.form\.get\("live_nay", type=int, default=0\)\s*live_abstain = request\.form\.get\("live_abstain", type=int, default=0\)',
    description_logic,
    content
)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated route with description capture")
