import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add fetching of new data
old_data_fetch = """        exceptions = data.get('exceptions', [])
        mapping = data.get('mapping', [])"""

new_data_fetch = """        exceptions = data.get('exceptions', [])
        mapping = data.get('mapping', [])
        arrears = data.get('arrears', [])
        arrangements = data.get('arrangements', [])
        owners = data.get('owners', [])"""

content = content.replace(old_data_fetch, new_data_fetch)

# 2. Update account creation logic
old_acc_creation = """        # 3. Create Accounts & Map
        for acc_data in accounts:
            owner_name = acc_data['owner']
            owner = None
            if owner_name:
                owner = RefMuniOwner.query.filter_by(name=owner_name).first()
                if not owner:
                    owner = RefMuniOwner(name=owner_name)
                    db.session.add(owner)
                    db.session.flush()

            muni_acc = BilMuniAccount(
                property_id=prop.id,
                account_number=acc_data['number'],
                is_bulk_account=acc_data['isBulk'],
                owner_id=owner.id if owner else None
            )"""

new_acc_creation = """        # 3. Create Accounts & Map
        for acc_data in accounts:
            acc_id = acc_data.get('id')
            
            # Find owner from Step 10, fallback to Step 2
            own_data = next((x for x in owners if x['account_id'] == acc_id), None)
            owner_name = own_data['name'] if own_data else acc_data.get('owner')
            owner_email = own_data['email'] if own_data else None
            
            owner = None
            if owner_name:
                owner = RefMuniOwner.query.filter_by(name=owner_name).first()
                if not owner:
                    owner = RefMuniOwner(name=owner_name)
                    db.session.add(owner)
                    db.session.flush()

            # Find arrears from Step 8
            arr_data = next((x for x in arrears if x['account_id'] == acc_id), None)
            arr_amt = arr_data['amount'] if arr_data else 0.0

            # Find arrangements from Step 9
            arg_data = next((x for x in arrangements if x['account_id'] == acc_id), None)
            arg_amt = arg_data['amount'] if arg_data else 0.0
            arg_dur = arg_data['duration'] if arg_data else 0

            muni_acc = BilMuniAccount(
                property_id=prop.id,
                account_number=acc_data['number'],
                is_bulk_account=acc_data['isBulk'],
                owner_id=owner.id if owner else None,
                muni_email=owner_email,
                rates_amount=arr_amt,
                arrangement_amount=arg_amt,
                arrangement_duration=arg_dur
            )"""

content = content.replace(old_acc_creation, new_acc_creation)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated backend to save arrears, arrangements, and owners.")
