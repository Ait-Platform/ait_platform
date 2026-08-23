import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_client = '''        # Mock finding or creating client
        client = MechClient.query.filter_by(name=customer_name).first()'''

new_client = '''        # Mock finding or creating client
        client = MechClient.query.filter_by(name=customer_name, user_id=current_user.id).first()'''

content = content.replace(old_client, new_client)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
