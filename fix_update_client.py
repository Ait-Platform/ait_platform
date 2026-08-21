import sys

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''    if name:
        client.name = name
    client.phone = phone
    client.email = email'''

new_target = '''    if name:
        client.name = name
    client.phone = phone
    client.email = email
    
    vin = request.form.get("vin")
    if job_id and vin:
        job_card = MechJobCard.query.get(job_id)
        if job_card and job_card.vehicle:
            job_card.vehicle.vin = vin'''

content = content.replace(target, new_target)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
