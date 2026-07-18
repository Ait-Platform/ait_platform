import re
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

replacement_old = '''    return render_template("program_billing/metsoa.html", 
                           tenant=None, 
                           property=prop,
                           month=month,
                           elec_rows=elec_rows,
                           elec_total=elec_total,
                           water_meters=water_meters,
                           water_total=water_total,
                           grand_total=grand_total)'''

replacement_new = '''    data = {
        "tenant": None,
        "property": prop,
        "month": month,
        "electricity": {
            "rows": elec_rows,
            "subtotal": elec_total
        },
        "water": {
            "meters": water_meters,
            "total": water_total
        },
        "grand_total": grand_total
    }
    return render_template("program_billing/metsoa.html", **data)'''

text = text.replace(replacement_old, replacement_new)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Fixed metsoa template arguments')
