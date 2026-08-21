import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''        job_cards = MechJobCard.query.join(MechVehicle).join(MechClient).filter(
            MechClient.id.in_([c.id for c in active_shop.clients])
        ).order_by(MechJobCard.created_at.desc()).all()'''

# For the prototype, we just query job cards linked to the user's clients
replacement = '''        job_cards = MechJobCard.query.join(MechVehicle).join(MechClient).filter(
            MechClient.user_id == current_user.id
        ).order_by(MechJobCard.created_at.desc()).all()
        # Fallback if clients weren't created with user_id
        if not job_cards:
            job_cards = MechJobCard.query.order_by(MechJobCard.created_at.desc()).limit(50).all()'''

content = content.replace(target, replacement)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated job_cards_list route")
