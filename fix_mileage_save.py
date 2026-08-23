import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_block = '''        if vin_number: vehicle.vin = vin_number
        if make: vehicle.make = make
        if model: vehicle.model = model
        if year: vehicle.year = year'''

new_block = '''        if vin_number: vehicle.vin = vin_number
        if make: vehicle.make = make
        if model: vehicle.model = model
        if year: vehicle.year = year
        if mileage and mileage.isdigit(): vehicle.mileage = int(mileage)'''

content = content.replace(old_block, new_block)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
