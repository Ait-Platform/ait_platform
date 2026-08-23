import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    'if job_card.vehicle.client.shop.user_id != current_user.id:',
    'if job_card.vehicle.client.user_id != current_user.id:'
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
