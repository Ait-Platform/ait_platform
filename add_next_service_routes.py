import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# For new_quote
content = content.replace(
    "job_card = MechJobCard(vehicle_id=vehicle.id)",
    "job_card = MechJobCard(vehicle_id=vehicle.id, next_service_due=request.form.get('next_service_due', ''))"
)

# For edit_quote
content = content.replace(
    "job_card.vehicle.vin = request.form.get('vin')",
    "job_card.vehicle.vin = request.form.get('vin')\n        job_card.next_service_due = request.form.get('next_service_due', '')"
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
