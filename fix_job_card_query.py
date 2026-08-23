import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_query = '''    if active_shop:
        job_cards = MechJobCard.query.filter(
            MechJobCard.shop_id == active_shop.id
        ).order_by(MechJobCard.created_at.desc()).all()'''

new_query = '''    if active_shop:
        job_cards = MechJobCard.query.join(MechVehicle).join(MechClient).filter(
            MechClient.user_id == current_user.id
        ).order_by(MechJobCard.created_at.desc()).all()'''

content = content.replace(old_query, new_query)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
