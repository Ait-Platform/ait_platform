import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

elec_old = '''        if meter_types.get(r.meter_id) == "electricity":'''
elec_new = '''        if (meter_types.get(r.meter_id) or "").lower() in ["electricity", "electrical"]:'''

water_old = '''        if meter_types.get(r.meter_id) == "water":'''
water_new = '''        if (meter_types.get(r.meter_id) or "").lower() == "water":'''

text = text.replace(elec_old, elec_new)
text = text.replace(water_old, water_new)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Updated utility type matching to be case-insensitive')
