import sys

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''    client = job_card.vehicle.client if job_card.vehicle else None
    if client and (not client.email or not client.phone):
        flash("Please fill in both the client's email and phone number before sending documents.", "warning")'''

new_target = '''    client = job_card.vehicle.client if job_card.vehicle else None
    if (client and (not client.email or not client.phone)) or (job_card.vehicle and not job_card.vehicle.vin):
        flash("Please fill in the client's email, phone, and vehicle VIN before generating documents.", "warning")'''

content = content.replace(target, new_target)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
