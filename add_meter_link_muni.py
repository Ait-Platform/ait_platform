import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_mapping = """                # Sub Account - use mapping
                acc_map = next((x for x in mapping if x['account_id'] == acc_data['id']), None)
                if acc_map:
                    w_id = acc_map.get('water')
                    if w_id and w_id in db_meters:
                        db_meters[w_id].municipal_bill_number = muni_acc.account_number
                    e_id = acc_map.get('elec')
                    if e_id and e_id in db_meters:
                        db_meters[e_id].municipal_bill_number = muni_acc.account_number"""

new_mapping = """                # Sub Account - use mapping
                acc_map = next((x for x in mapping if x['account_id'] == acc_data['id']), None)
                if acc_map:
                    w_id = acc_map.get('water')
                    if w_id and w_id in db_meters:
                        db_meters[w_id].municipal_bill_number = muni_acc.account_number
                        muni_acc.water_meter_id = db_meters[w_id].id
                    e_id = acc_map.get('elec')
                    if e_id and e_id in db_meters:
                        db_meters[e_id].municipal_bill_number = muni_acc.account_number
                        muni_acc.elec_meter_id = db_meters[e_id].id
                db.session.flush()"""

if old_mapping in content:
    content = content.replace(old_mapping, new_mapping)
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Updated routes to link meter IDs directly to BilMuniAccount")
else:
    print("Could not find old_mapping in routes.py")
