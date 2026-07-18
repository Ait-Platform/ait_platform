import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Update the parsing of exceptions to include the dates
old_exception_parse = """                stolen_meter = BilMeter(
                    meter_number=stolen_num,
                    utility_type=rep_meter.utility_type,
                    status='stolen',
                    municipal_bill_number=rep_meter.municipal_bill_number,
                    replacement_for_meter_id=rep_meter.id,
                    pointing_to='Stolen Exception'
                )"""

new_exception_parse = """                # Attempt to parse dates
                from datetime import datetime
                d_stolen = None
                if exc.get('date_stolen'):
                    try: d_stolen = datetime.strptime(exc.get('date_stolen'), '%Y-%m-%d').date()
                    except: pass
                
                d_replaced = None
                if exc.get('date_replaced'):
                    try: d_replaced = datetime.strptime(exc.get('date_replaced'), '%Y-%m-%d').date()
                    except: pass

                stolen_meter = BilMeter(
                    meter_number=stolen_num,
                    utility_type=rep_meter.utility_type,
                    status='stolen',
                    municipal_bill_number=rep_meter.municipal_bill_number,
                    replacement_for_meter_id=rep_meter.id,
                    pointing_to='Stolen Exception',
                    date_stolen=d_stolen,
                    date_replaced=d_replaced
                )"""

if old_exception_parse in content:
    content = content.replace(old_exception_parse, new_exception_parse)
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Updated routes.py to save dates.")
else:
    print("Could not find exception parsing in routes.py")
