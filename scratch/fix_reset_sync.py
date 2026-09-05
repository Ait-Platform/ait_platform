import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    "SaceWorkshopInteraction.query.filter_by(workshop_session_id='demo-session-1').delete()",
    "SaceWorkshopInteraction.query.filter_by(workshop_session_id='demo-session-1').delete(synchronize_session=False)"
)

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
