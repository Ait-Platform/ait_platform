with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

import re
old_block = re.search(r'# 2\. Process Owners.*?(?=# Attach Rates)', text, re.DOTALL)
if old_block:
    new_code = """# 2. Process Owners
        owner_map = {} # acc.id (e.g. 'acc_0') -> owner_obj.id
        owner_data_map = {} # acc.id -> {'email': ..., 'address': ...}
        for o_data in data.get('owners', []):
            name = o_data.get('name', '').strip().title()
            acc_id = o_data.get('account_id')
            email = o_data.get('email', '').strip()
            address = o_data.get('address', '').strip()
            if name and acc_id:
                owner = RefMuniOwner.query.filter_by(name=name).first()
                if not owner:
                    owner = RefMuniOwner(name=name)
                    db.session.add(owner)
                    db.session.flush()
                owner_map[acc_id] = owner.id
                owner_data_map[acc_id] = {'email': email, 'address': address}

        # 3. Process Accounts & attach rates/arrears/arrangements
        acc_obj_map = {} # acc.id -> BilMuniAccount
        for a_data in data.get('accounts', []):
            acc_num = a_data.get('number', '').strip()
            acc_id = a_data.get('id')
            if acc_num and acc_id:
                acc = BilMuniAccount(
                    property_id=prop.id,
                    account_number=acc_num,
                    is_bulk_account=True if a_data.get('isBulk') else False
                )
                if acc_id in owner_map:
                    acc.owner_id = owner_map[acc_id]
                if acc_id in owner_data_map:
                    acc.muni_email = owner_data_map[acc_id].get('email')
                    acc.owner_address = owner_data_map[acc_id].get('address')
                db.session.add(acc)
                acc_obj_map[acc_id] = acc
        
        db.session.flush()

        """
    text = text.replace(old_block.group(0), new_code)
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Fixed email and address mapping!")
else:
    print("Failed to match block.")
