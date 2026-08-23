import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the vehicle search logic in repair_tracker_api
regex = r'(# Find vehicles matching this reg\s*vehicles = MechVehicle\.query\.join\(MechJobCard\)\.filter\(\s*MechVehicle\.license_plate\.ilike\(f"%\{reg_number\.strip\(\)\}%"\)\s*\)\.all\(\))'

new_logic = '''# Find vehicles matching reg, client name, or job number
    from app.models.mechanic import MechClient
    search_term = f"%{reg_number.strip()}%"
    
    vehicles = MechVehicle.query.join(MechJobCard).join(MechClient).filter(
        db.or_(
            MechVehicle.license_plate.ilike(search_term),
            MechClient.name.ilike(search_term),
            MechJobCard.job_number.ilike(search_term)
        )
    ).all()'''

content = re.sub(regex, new_logic, content)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
