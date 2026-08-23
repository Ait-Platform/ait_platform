import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''    job_card.vehicle.make = request.form.get("make")
    job_card.vehicle.model = request.form.get("model")
    job_card.vehicle.vin = request.form.get("vin")
    
    year_str = request.form.get("year")
    if year_str and year_str.isdigit():
        job_card.vehicle.year = int(year_str)
    else:
        job_card.vehicle.year = None'''

content = content.replace(
    '''    job_card.vehicle.make = request.form.get("make")
    job_card.vehicle.model = request.form.get("model")
    job_card.vehicle.vin = request.form.get("vin")''',
    replacement
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
