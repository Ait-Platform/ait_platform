import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

regex = r'search_term = f"%\{reg_number\.strip\(\)\}%"\s*vehicles = MechVehicle\.query\.join\(MechJobCard\)\.join\(MechClient\)\.filter\(\s*db\.or_\(\s*MechVehicle\.license_plate\.ilike\(search_term\),\s*MechClient\.name\.ilike\(search_term\),\s*MechJobCard\.job_number\.ilike\(search_term\)\s*\)\s*\)\.all\(\)'

new_logic = '''search_term = f"%{reg_number.strip()}%"
    clean_reg = reg_number.strip().replace(" ", "")
    search_term_clean = f"%{clean_reg}%"
    
    vehicles = MechVehicle.query.join(MechJobCard).join(MechClient).filter(
        db.or_(
            MechVehicle.license_plate.ilike(search_term),
            MechVehicle.license_plate.ilike(search_term_clean),
            db.func.replace(MechVehicle.license_plate, ' ', '').ilike(search_term_clean),
            MechClient.name.ilike(search_term),
            MechJobCard.job_number.ilike(search_term)
        ),
        MechClient.user_id == current_user.id
    ).all()'''

content = re.sub(regex, new_logic, content)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
